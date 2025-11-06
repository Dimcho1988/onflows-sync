\
import streamlit as st
import os, time, math, json
import requests
from urllib.parse import urlencode, urlparse, parse_qs
from dateutil import tz
from datetime import datetime, timedelta
from supabase import create_client

st.set_page_config(page_title="onFlows — Strava Sync (Streamlit only)", layout="wide")

# --- Secrets / Config ---
STRAVA_CLIENT_ID = st.secrets.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = st.secrets.get("STRAVA_CLIENT_SECRET")
APP_BASE_URL = st.secrets.get("APP_BASE_URL")  # e.g. https://your-app.streamlit.app
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_SERVICE_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def strava_auth_url():
    params = {
        "client_id": STRAVA_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": APP_BASE_URL,  # Strava will append ?code=...
        "approval_prompt": "auto",
        "scope": "read,activity:read_all,profile:read_all"
    }
    return "https://www.strava.com/oauth/authorize?" + urlencode(params)

def exchange_code_for_token(code: str):
    token_url = "https://www.strava.com/oauth/token"
    r = requests.post(token_url, data={
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code"
    }, timeout=30)
    r.raise_for_status()
    return r.json()

def refresh_token_if_needed(tok: dict):
    now = int(time.time())
    if tok["expires_at"] - now < 60:
        r = requests.post("https://www.strava.com/oauth/token", data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"]
        }, timeout=30)
        r.raise_for_status()
        return r.json()
    return tok

def save_tokens(athlete_id: int, tok: dict, scope: str):
    sb.table("oauth_tokens").upsert({
        "athlete_id": athlete_id,
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token", ""),
        "expires_at": tok["expires_at"],
        "scope": scope
    }).execute()

def get_tokens(athlete_id: int):
    res = sb.table("oauth_tokens").select("*").eq("athlete_id", athlete_id).execute()
    data = res.data or []
    return data[0] if data else None

def strava_get(url: str, token: str, params=None):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    if r.status_code == 429:
        raise RuntimeError("Rate limit hit")
    r.raise_for_status()
    return r.json()

def fetch_athlete(token):
    return strava_get("https://www.strava.com/api/v3/athlete", token)

def fetch_activities_since(token, after_epoch: int):
    # pagesize 50 to reduce calls
    acts = []
    page = 1
    while True:
        batch = strava_get("https://www.strava.com/api/v3/athlete/activities", token, {
            "after": after_epoch,
            "page": page,
            "per_page": 50
        })
        if not batch:
            break
        acts.extend(batch)
        page += 1
        if page > 10:
            break  # safety
    return acts

def fetch_streams(token, activity_id: int):
    keys = "time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,grade_smooth"
    return strava_get(f"https://www.strava.com/api/v3/activities/{activity_id}/streams", token, {
        "keys": keys,
        "key_by_type": "true"
    })

def light_validate_and_transform(streams: dict):
    t = streams.get("time", {}).get("data") or []
    dist = streams.get("distance", {}).get("data") or []
    latlng = streams.get("latlng", {}).get("data") or []
    alt = streams.get("altitude", {}).get("data") or []
    vel = streams.get("velocity_smooth", {}).get("data") or []
    hr = streams.get("heartrate", {}).get("data") or []
    cad = streams.get("cadence", {}).get("data") or []
    grade = streams.get("grade_smooth", {}).get("data") or []

    rows = []
    for i in range(len(t)):
        # basic filters
        hr_v = hr[i] if i < len(hr) else None
        spd = vel[i] if i < len(vel) else None
        if hr_v is not None and (hr_v < 40 or hr_v > 230):
            hr_v = None
        if spd is not None and spd < 0.2:
            spd = 0.0
        lat, lon = (None, None)
        if i < len(latlng) and isinstance(latlng[i], list) and len(latlng[i]) == 2:
            lat, lon = latlng[i]
        rows.append({
            "ts_rel_s": int(t[i]),
            "dist_m": float(dist[i]) if i < len(dist) else None,
            "speed_ms": float(spd) if spd is not None else None,
            "hr_bpm": float(hr_v) if hr_v is not None else None,
            "altitude_m": float(alt[i]) if i < len(alt) else None,
            "cadence_spm": float(cad[i]) if i < len(cad) else None,
            "lat": float(lat) if lat is not None else None,
            "lon": float(lon) if lon is not None else None
        })
    return rows

def insert_activity_meta(act, athlete_id):
    sb.table("activities").upsert({
        "activity_id": act["id"],
        "athlete_id": athlete_id,
        "sport_type": act.get("sport_type") or act.get("type"),
        "start_date_utc": act.get("start_date"),
        "start_date_local": act.get("start_date_local"),
        "elapsed_time_s": act.get("elapsed_time"),
        "moving_time_s": act.get("moving_time"),
        "distance_m": act.get("distance"),
        "avg_speed_ms": act.get("average_speed"),
        "avg_hr_bpm": act.get("average_heartrate"),
        "name": act.get("name"),
        "ingest_status": "active"
    }).execute()

def insert_stream_rows(activity_id: int, rows):
    # insert in chunks of 500
    chunk = []
    for r in rows:
        r2 = dict(r)
        r2["activity_id"] = activity_id
        chunk.append(r2)
        if len(chunk) >= 500:
            sb.table("raw_streams").insert(chunk).execute()
            chunk = []
    if chunk:
        sb.table("raw_streams").insert(chunk).execute()

def last_sync_epoch_default():
    # default: 30 days ago
    return int((datetime.utcnow() - timedelta(days=30)).timestamp())

def do_full_sync(tok: dict, athlete_id: int, after_epoch: int = None):
    tok = refresh_token_if_needed(tok)
    save_tokens(athlete_id, tok, tok.get("scope",""))
    access = tok["access_token"]
    after_epoch = after_epoch or last_sync_epoch_default()

    acts = fetch_activities_since(access, after_epoch)
    for a in acts:
        insert_activity_meta(a, athlete_id)
        try:
            streams = fetch_streams(access, a["id"])
            rows = light_validate_and_transform(streams)
            if rows:
                insert_stream_rows(a["id"], rows)
        except Exception as e:
            st.warning(f"Streams failed for activity {a['id']}: {e}")

    return len(acts)

# ---------------- UI ----------------

st.title("onFlows — Strava sync (Streamlit only)")

qs_code = st.query_params.get("code", None)
qs_scope = st.query_params.get("scope", None)

if "athlete" not in st.session_state:
    st.session_state["athlete"] = None

if qs_code and not st.session_state.get("athlete"):
    with st.spinner("Completing Strava OAuth..."):
        try:
            tok = exchange_code_for_token(qs_code)
            athlete = tok.get("athlete", {})
            athlete_id = int(athlete.get("id"))
            # store tokens
            save_tokens(athlete_id, tok, qs_scope or "")
            # optional: store athlete profile
            sb.table("athletes").upsert({
                "athlete_id": athlete_id,
                "firstname": athlete.get("firstname"),
                "lastname": athlete.get("lastname"),
                "profile": athlete.get("profile")
            }).execute()
            st.session_state["athlete"] = {"id": athlete_id, "name": f"{athlete.get('firstname','')} {athlete.get('lastname','')}".strip()}
            # initial sync
            n = do_full_sync(tok, athlete_id)
            st.success(f"Connected as {st.session_state['athlete']['name']} (id {athlete_id}). Synced {n} recent activities.")
            # clear code from URL
            st.query_params.clear()
        except Exception as e:
            st.error(f"OAuth failed: {e}")

if not st.session_state["athlete"]:
    st.write("Connect your Strava account to start syncing activities to Supabase.")
    if st.button("Connect with Strava"):
        st.markdown(f"[Click to continue →]({strava_auth_url()})")
else:
    st.info(f"Connected: {st.session_state['athlete']['name']} (id {st.session_state['athlete']['id']})")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sync latest (last 7 days)"):
            athlete_id = st.session_state["athlete"]["id"]
            tok = get_tokens(athlete_id)
            if not tok:
                st.error("Missing tokens. Reconnect with Strava.")
            else:
                try:
                    n = do_full_sync(tok, athlete_id, after_epoch=int((datetime.utcnow()-timedelta(days=7)).timestamp()))
                    st.success(f"Synced {n} activities (7 days).")
                except Exception as e:
                    st.error(f"Sync failed: {e}")
    with col2:
        if st.button("Sync last 30 days"):
            athlete_id = st.session_state["athlete"]["id"]
            tok = get_tokens(athlete_id)
            if not tok:
                st.error("Missing tokens. Reconnect with Strava.")
            else:
                try:
                    n = do_full_sync(tok, athlete_id, after_epoch=int((datetime.utcnow()-timedelta(days=30)).timestamp()))
                    st.success(f"Synced {n} activities (30 days).")
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    st.caption("Streams are lightly filtered (HR 40–230 bpm; speed <0.2 m/s set to 0). Data is saved into Supabase tables `activities` and `raw_streams`.")
