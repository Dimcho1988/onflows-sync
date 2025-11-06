import json
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
import streamlit as st
from supabase import create_client


# ==============================
# Secrets / Config
# ==============================
APP_BASE_URL = st.secrets["app"]["base_url"].rstrip("/")
STRAVA_CLIENT_ID = str(st.secrets["strava"]["client_id"])
STRAVA_CLIENT_SECRET = st.secrets["strava"]["client_secret"]
# ВАЖНО: това е БАЗОВИЯТ URL (без /oauth/callback)
STRAVA_REDIRECT_URI = st.secrets["strava"]["oauth_redirect_uri"].rstrip("/")

SUPABASE_URL = st.secrets["supabase"]["url"]
# ако имаш service_key в secrets — ползва него; иначе ползва anon_key
SUPABASE_KEY = st.secrets["supabase"].get("service_key", st.secrets["supabase"]["anon_key"])

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="onFlows • Strava → Supabase", layout="centered")


# ==============================
# Helpers
# ==============================
def strava_auth_url() -> str:
    """Генерира OAuth линк към Strava, с redirect обратно към базовия URL."""
    params = {
        "client_id": STRAVA_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": STRAVA_REDIRECT_URI,  # <= базов URL (без /oauth/callback)
        "scope": "read,activity:read_all",
        "approval_prompt": "auto",
    }
    return f"https://www.strava.com/oauth/authorize?{urlencode(params)}"


def exchange_code_for_token(code: str) -> dict:
    """Размяна на code -> access_token в Strava."""
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": STRAVA_REDIRECT_URI,  # трябва да съвпада 1:1
        },
        timeout=12,
    )
    # Вадим грешката директно в UI, за да не "виси".
    if resp.status_code != 200:
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}
        raise RuntimeError(f"Strava token exchange failed ({resp.status_code}): {payload}")
    return resp.json()


def refresh_access_token(refresh_token: str) -> dict:
    """Освежаване на access_token по refresh_token."""
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=12,
    )
    if resp.status_code != 200:
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}
        raise RuntimeError(f"Strava token refresh failed ({resp.status_code}): {payload}")
    return resp.json()


def upsert_tokens_to_db(user_key: str, tok: dict) -> None:
    """
    Записва/ъпсъртва токените в таблица 'strava_tokens'.
    Очаквана схема (минимум колони):
      - user_key (text) PRIMARY KEY
      - athlete_id (bigint)
      - access_token (text)
      - refresh_token (text)
      - expires_at (bigint)  # UNIX сек.
      - inserted_at (timestamptz) default now()
      - updated_at (timestamptz) default now()
    """
    row = {
        "user_key": user_key,
        "athlete_id": tok.get("athlete", {}).get("id"),
        "access_token": tok["access_token"],
        "refresh_token": tok["refresh_token"],
        "expires_at": tok["expires_at"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    # upsert по user_key
    supabase.table("strava_tokens").upsert(row, on_conflict="user_key").execute()


def get_tokens_from_db(user_key: str) -> dict | None:
    """Дърпа запис от 'strava_tokens' по user_key."""
    res = supabase.table("strava_tokens").select("*").eq("user_key", user_key).limit(1).execute()
    if res.data:
        return res.data[0]
    return None


def get_valid_access_token(user_key: str) -> str | None:
    """Връща валиден access_token; ако е изтекъл — освежава и записва."""
    row = get_tokens_from_db(user_key)
    if not row:
        return None
    now = int(time.time())
    if row["expires_at"] and row["expires_at"] - now < 60:
        # refresh
        refreshed = refresh_access_token(row["refresh_token"])
        upsert_tokens_to_db(user_key, refreshed)
        return refreshed["access_token"]
    return row["access_token"]


def fetch_activities(access_token: str, page: int = 1, per_page: int = 30) -> list[dict]:
    """Дърпа списък с активности от Strava API v3 /athlete/activities."""
    resp = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"page": page, "per_page": per_page},
        timeout=15,
    )
    if resp.status_code != 200:
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}
        raise RuntimeError(f"Strava /activities failed ({resp.status_code}): {payload}")
    return resp.json()


# ==============================
# UI
# ==============================
st.title("onFlows • Strava → Supabase\n(browser-only)")
st.caption("Натисни „Свържи Strava“, за да продължиш.")

# „user_key“ = нещо, по което ще различаваме потребителите.
# За лична употреба може да е фиксиран стринг; ако имаш auth — ползвай user id/email.
USER_KEY = "dimcho-local"

# 1) Ако Strava ни е върнал ?code=..., правим размяната веднага
qs = dict(st.query_params)
if "code" in qs:
    code = qs["code"]
    try:
        token_payload = exchange_code_for_token(code)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Пазим токените в session_state (за бърза работа)
    st.session_state["strava_tokens"] = token_payload

    # Опит за запис в DB (ако таблицата съществува)
    try:
        upsert_tokens_to_db(USER_KEY, token_payload)
        st.success("Токените са записани в Supabase.")
    except Exception as e:
        st.warning(f"Записът в Supabase не мина (може да липсва таблица 'strava_tokens'): {e}")

    # Премахни ?code от адреса (изчиства URL без reload)
    st.query_params.clear()

# 2) Ако вече имаме токени (в сесия или в DB), показваме бутон за тест и дърпане на активности
tokens = st.session_state.get("strava_tokens")
if not tokens:
    # опитай да вземеш от DB
    try:
        row = get_tokens_from_db(USER_KEY)
    except Exception:
        row = None
    if row:
        tokens = {
            "access_token": row.get("access_token"),
            "refresh_token": row.get("refresh_token"),
            "expires_at": row.get("expires_at"),
            "athlete": {"id": row.get("athlete_id")},
        }
        st.session_state["strava_tokens"] = tokens

# 3) Стартов екран: бутон за OAuth
if not tokens:
    st.link_button("Свържи Strava", strava_auth_url())
    st.stop()

# 4) Показваме малко инфо за логнатия атлет и токена
cols = st.columns(3)
with cols[0]:
    st.success("Свързан със Strava")
with cols[1]:
    st.write("Athlete ID:", tokens.get("athlete", {}).get("id"))
with cols[2]:
    exp = tokens.get("expires_at") or 0
    st.write("Token expires:", datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

# 5) Освежи токена при нужда
if tokens.get("expires_at", 0) - int(time.time()) < 60:
    try:
        refreshed = refresh_access_token(tokens["refresh_token"])
        st.session_state["strava_tokens"] = refreshed
        try:
            upsert_tokens_to_db(USER_KEY, refreshed)
        except Exception:
            pass
        st.info("Access token беше освежен.")
    except Exception as e:
        st.error(f"Проблем при освежаване на токена: {e}")
        st.stop()

# 6) Демонстрация: дърпане на последните активности
st.subheader("Тест: последни Strava активности")
if st.button("Дръпни 10 активности"):
    try:
        at = st.session_state["strava_tokens"]["access_token"]
        acts = fetch_activities(at, page=1, per_page=10)
        st.success(f"ОК: получени {len(acts)} активности")
        if acts:
            # Показваме кратък списък
            pretty = [
                {
                    "name": a.get("name"),
                    "type": a.get("sport_type") or a.get("type"),
                    "start_date": a.get("start_date_local"),
                    "distance_km": round((a.get("distance") or 0) / 1000, 2),
                    "moving_time_min": round((a.get("moving_time") or 0) / 60, 1),
                    "avg_hr": a.get("average_heartrate"),
                }
                for a in acts
            ]
            st.dataframe(pretty, use_container_width=True)
    except Exception as e:
        st.error(str(e))

st.divider()
with st.expander("Диагностика (скрий след като всичко заработи)"):
    st.write("Base URL:", APP_BASE_URL)
    st.write("Redirect URI:", STRAVA_REDIRECT_URI)
    st.write("Query params:", dict(st.query_params))
    try:
        st.write("Supabase ping:", supabase.auth.get_session() is not None or "client init OK")
    except Exception as e:
        st.write("Supabase client error:", e)
