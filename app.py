import streamlit as st
import time, requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
from supabase import create_client
from etl import validate_transform, detect_artifacts  # local etl.py

st.set_page_config(page_title="onFlows - Strava sync v2", layout="wide")

# ---- Secrets ----
STRAVA_CLIENT_ID     = st.secrets.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = st.secrets.get("STRAVA_CLIENT_SECRET")
APP_BASE_URL         = st.secrets.get("APP_BASE_URL")          # e.g. https://onflows-sync-2.streamlit.app
SUPABASE_URL         = st.secrets.get("SUPABASE_URL")
SUPABASE_KEY         = st.secrets.get("SUPABASE_SERVICE_KEY")

missing = [k for k,v in {
    "STRAVA_CLIENT_ID":STRAVA_CLIENT_ID,
    "STRAVA_CLIENT_SECRET":STRAVA_CLIENT_SECRET,
    "APP_BASE_URL":APP_BASE_URL,
    "SUPABASE_URL":SUPABASE_URL,
    "SUPABASE_SERVICE_KEY":SUPABASE_KEY}.items() if not v]
if missing:
    st.error("Missing secrets: " + ", ".join(missing))
    st.stop()

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Helpers ----
def strava_auth_url():
    params = {
        "client_id": STRAVA_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": APP_BASE_URL,
        "approval_prompt": "auto",
        "scope": "read,activity:read_all,profile:read_all",
    }
    return "https://www.strava.com/oauth/authorize?" + urlencode(params)

def exchange_code_for_token(code):
    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
    }, timeout=30)
    r.raise_for_status()
    return r.json()

def refresh_if_needed(tok):
    now = int(time.time())
    if tok["expires_at"] - now < 60:
        r = requests.post("https://www.strava.com/oauth/token", data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"],
        }, timeout=30)
        r.raise_for_status()
        return r.json()
    return tok

def save_tokens(athlete_id, tok, scope):
    sb.table("oauth_tokens").upsert({
        "athlete_id": int(athlete_id),
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token",""),
        "expires_at": int(tok["expires_at"]),
        "scope": scope or "",
    }).execute()

def get_tokens(athlete_id):
    res = sb.table("oauth_tokens").select("*").eq("athlete_id", int(athlete_id)).execute()
    data = res.data or []
    return data[0] if data else None

def sget(url, token, params=None):
    r = requests.get(url, headers={"Authorization": "Bearer " + token}, params=params or {}, timeout=30)
    if r.status_code == 429:
        raise RuntimeError("Strava rate limit")
    r.raise_for_status()
    return r.json()

def fetch_activities_since(token, after_epoch):
    acts, page = [], 1
    while True:
        batch = sget("https://www.strava.com/api/v3/athlete/activities", token, {
            "after": after_epoch, "page": page, "per_page": 50,
        })
        if not batch:
            break
        acts.extend(batch)
        page += 1
        if page > 10:
            break
    return acts

def fetch_streams(token, activity_id):
    keys = "time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,grade_smooth"
    return sget("https://www.strava.com/api/v3/activities/%s/streams" % activity_id, token, {
        "keys": keys, "key_by_type": "true",
    })

def insert_activity_meta(act, athlete_id):
    sb.table("activities").upsert({
        "activity_id": int(act["id"]),
        "athlete_id": int(athlete_id),
        "sport_type": (act.get("sport_type") or act.get("type") or "")[:40],
        "start_date_utc": act.get("start_date"),
        "start_date_local": act.get("start_date_local"),
        "elapsed_time_s": act.get("elapsed_time"),
        "moving_time_s": act.get("moving_time"),
        "distance_m": act.get("distance"),
        "avg_speed_ms": act.get("average_speed"),
        "avg_hr_bpm": act.get("average_heartrate"),
        "name": (act.get("name") or "")[:120],   # cut to ASCII-safe length
        "ingest_status": "active",
    }).execute()

def insert_stream_rows(activity_id, rows):
    batch = []
    for r in rows:
        rr = dict(r); rr["activity_id"] = int(activity_id)
        batch.append(rr)
        if len(batch) >= 500:
            sb.table("raw_streams").insert(batch).execute()
            batch = []
    if batch:
        sb.table("raw_streams").insert(batch).execute()

def insert_artifacts(activity_id, arts):
    if not arts: return
    batch = []
    for a in arts:
        batch.append({
            "activity_id": int(activity_id),
            "ts_rel_s_from": int(a["ts_rel_s_from"]),
            "ts_rel_s_to": int(a["ts_rel_s_to"]),
            "kind": (a.get("kind") or "")[:40],
            "severity": int(a.get("severity", 1)),
            "note": (a.get("note","") or "")[:160],
        })
        if len(batch) >= 500:
            sb.table("stream_artifacts").insert(batch).execute()
            batch = []
    if batch:
        sb.table("stream_artifacts").insert(batch).execute()

def last_sync_epoch(days=30):
    return int((datetime.utcnow() - timedelta(days=days)).timestamp())

def sync_after(athlete_id, token_dict, days=30):
    token_dict = refresh_if_needed(token_dict)
    save_tokens(athlete_id, token_dict, token_dict.get("scope",""))
    access = token_dict["access_token"]

    acts = fetch_activities_since(access, last_sync_epoch(days))
    imported = 0
    for a in acts:
        insert_activity_meta(a, athlete_id)
        try:
            streams = fetch_streams(access, a["id"])
            rows = validate_transform(streams)
            if rows:
                insert_stream_rows(a["id"], rows)
                insert_artifacts(a["id"], detect_artifacts(rows))
                try:
                    sb.rpc("rebuild_agg_30s", {"p_activity_id": int(a["id"])}).execute()
                except Exception:
                    st.warning("rebuild_agg_30s failed for activity %s" % a["id"])
            sb.table("activities").update({"ingest_status": "done"}).eq("activity_id", int(a["id"])).execute()
            imported += 1
        except Exception:
            sb.table("activities").update({"ingest_status": "error"}).eq("activity_id", int(a["id"])).execute()
            st.warning("stream import failed for activity %s" % a["id"])
    return imported

# ---- UI ----
st.title("onFlows - Strava sync (v2)")

# Robust query param read (works on older Streamlit too)
_qp = st.experimental_get_query_params()
qs_code  = (_qp.get("code")  or [None])[0]
qs_scope = (_qp.get("scope") or [None])[0]

if "athlete" not in st.session_state:
    st.session_state["athlete"] = None

if qs_code and not st.session_state.get("athlete"):
    with st.spinner("Completing Strava OAuth..."):
        try:
            tok = exchange_code_for_token(qs_code)
            athlete = tok.get("athlete") or {}
            athlete_id = int(athlete.get("id"))

            # NOTE: no text inserts here; only tokens (avoid unicode edge cases)
            save_tokens(athlete_id, tok, qs_scope or "")
            st.session_state["athlete"] = {"id": athlete_id, "name": "Athlete %s" % athlete_id}

            n = sync_after(athlete_id, tok, days=30)
            st.success("Connected (athlete_id %s). Synced %s activities (last 30 days)." % (athlete_id, n))
        except Exception:
            st.error("OAuth failed. Please try again.")
        finally:
            st.experimental_set_query_params()

if not st.session_state["athlete"]:
    st.write("Connect your Strava account to start syncing activities to Supabase.")
    if st.button("Connect with Strava"):
        st.markdown("[Click to continue ->](%s)" % strava_auth_url())
else:
    st.info("Connected: %s (id %s)" % (st.session_state["athlete"]["name"], st.session_state["athlete"]["id"]))
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Sync last 7 days"):
            tok = get_tokens(st.session_state["athlete"]["id"])
            if not tok:
                st.error("Missing tokens")
            else:
                n = sync_after(st.session_state["athlete"]["id"], tok, days=7)
                st.success("Synced %s activities (7 days)" % n)
    with c2:
        if st.button("Sync last 30 days"):
            tok = get_tokens(st.session_state["athlete"]["id"])
            if not tok:
                st.error("Missing tokens")
            else:
                n = sync_after(st.session_state["athlete"]["id"], tok, days=30)
                st.success("Synced %s activities (30 days)" % n)
    with c3:
        if st.button("Rebuild 30s aggregates (all activities)"):
            with st.spinner("Rebuilding..."):
                try:
                    res = sb.table("activities").select("activity_id").eq("athlete_id", st.session_state["athlete"]["id"]).execute()
                    ids = [r["activity_id"] for r in (res.data or [])]
                    for aid in ids:
                        sb.rpc("rebuild_agg_30s", {"p_activity_id": int(aid)}).execute()
                    st.success("Rebuilt aggregates for %s activities." % len(ids))
                except Exception:
                    st.error("Rebuild failed")

    st.caption("Artifacts: HR outliers, zero-speed pauses >30s, GPS jumps >50m, missing data >20%. Tables: activities, raw_streams, stream_artifacts, agg_streams_30s.")
