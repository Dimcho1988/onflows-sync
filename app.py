import time
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
from supabase import create_client, Client

# --------------------------
# Конфигурация от secrets
# --------------------------
STRAVA_CLIENT_ID = st.secrets["strava"]["client_id"]
STRAVA_CLIENT_SECRET = st.secrets["strava"]["client_secret"]
STRAVA_REFRESH_TOKEN = st.secrets["strava"]["refresh_token"]

SUPABASE_URL = st.secrets["supabase"]["url"]
SUPABASE_KEY = st.secrets["supabase"]["service_role_key"]  # може да е anon или service_role

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------
# ВРЕМЕНЕН БЛОК: взимане на refresh_token от ?code=... URL
# (след като вземем новия refresh_token и го сложим в secrets,
# този блок може спокойно да се изтрие)
# ---------------------------------------------------------
query_params = st.experimental_get_query_params()
if "code" in query_params:
    auth_code = query_params["code"][0]
    st.write("Получен code от Strava:", auth_code)

    token_url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": auth_code,
    }
    resp = requests.post(token_url, data=data, timeout=10)
    st.write("DEBUG exchange status:", resp.status_code)
    st.write("DEBUG exchange body:", resp.text)

    if resp.ok:
        token_info = resp.json()
        st.success("Нов refresh_token (копирай и го сложи в secrets):")
        st.code(token_info.get("refresh_token", "няма refresh_token"))
        st.write("Scope на токена:", token_info.get("scope"))
# ---------------------------------------------------------


# --------------------------
# STRAVA helper-и
# --------------------------
def get_strava_access_token() -> str:
    """Взима нов access_token от Strava чрез refresh_token."""
    token_url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN,
    }
    resp = requests.post(token_url, data=data, timeout=10)

    # DEBUG
    st.write("DEBUG /oauth/token status:", resp.status_code)
    st.write("DEBUG /oauth/token body:", resp.text)

    resp.raise_for_status()
    token_info = resp.json()
    return token_info["access_token"]


def fetch_activities_since(access_token: str, after_ts: int):
    """Връща всички активности след даден UNIX timestamp."""
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}

    all_activities = []
    page = 1
    per_page = 50

    while True:
        params = {"after": after_ts, "page": page, "per_page": per_page}
        r = requests.get(url, headers=headers, params=params, timeout=10)

        # DEBUG за грешки
        if r.status_code != 200:
            st.error(f"Strava /athlete/activities ERROR {r.status_code}: {r.text}")
            r.raise_for_status()

        chunk = r.json()
        if not chunk:
            break
        all_activities.extend(chunk)
        page += 1

    return all_activities


def fetch_activity_streams(access_token: str, activity_id: int):
    """Дърпа стриймовете за дадена активност."""
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "keys": "time,distance,altitude,heartrate,cadence,latlng",
        "key_by_type": "true",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()  # dict: тип -> {original_size, resolution, data}


# --------------------------
# SUPABASE helper-и
# --------------------------
def get_last_activity_start_date() -> datetime | None:
    """Последната start_date от activities (ако има такава)."""
    try:
        res = (
            supabase.table("activities")
            .select("start_date")
            .order("start_date", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as e:
        st.error(f"Supabase error при get_last_activity_start_date: {e}")
        return None

    data = res.data
    if not data:
        return None
    return datetime.fromisoformat(data[0]["start_date"])


def upsert_activity(act: dict) -> int:
    """Записва/обновява активност и връща локалното id в activities."""
    row = {
        "user_id": 1,  # за сега твоя потребител
        "strava_activity_id": act["id"],
        "name": act.get("name"),
        "sport_type": act.get("sport_type"),
        "distance": act.get("distance"),
        "moving_time": act.get("moving_time"),
        "elapsed_time": act.get("elapsed_time"),
        "start_date": act.get("start_date"),
        "timezone": act.get("timezone"),
        "average_speed": act.get("average_speed"),
        "max_speed": act.get("max_speed"),
        "has_heartrate": act.get("has_heartrate", False),
        "average_heartrate": act.get("average_heartrate"),
        "max_heartrate": act.get("max_heartrate"),
    }

    try:
        res = (
            supabase.table("activities")
            .upsert(row, on_conflict="strava_activity_id")
            .execute()
        )
    except Exception as e:
        st.error(f"Supabase error при upsert_activity (activities): {e}")
        raise

    return res.data[0]["id"]


def save_streams(activity_id: int, streams: dict) -> int:
    """Записва стриймовете в activity_streams."""
    try:
        supabase.table("activity_streams").delete().eq("activity_id", activity_id).execute()
    except Exception as e:
        st.error(f"Supabase error при изтриване на stream-ове: {e}")
        raise

    rows = []
    for stream_type, payload in streams.items():
        rows.append(
            {
                "activity_id": activity_id,
                "stream_type": stream_type,
                "data": payload.get("data", []),
            }
        )

    if not rows:
        return 0

    try:
        res = supabase.table("activity_streams").insert(rows).execute()
    except Exception as e:
        st.error(f"Supabase error при insert в activity_streams: {e}")
        raise

    return len(res.data)


# --------------------------
# Основен sync pipeline
# --------------------------
def sync_from_strava():
    access_token = get_strava_access_token()

    # Ако вече имаме активности -> от последната дата - 60 сек
    # Ако нямаме -> последните 30 дни
    last_dt = get_last_activity_start_date()
    if last_dt:
        after_ts = int(last_dt.timestamp()) - 60
        info_text = f"Синхронирам от последната записана активност: {last_dt}."
    else:
        after_ts = int((datetime.now(timezone.utc) - timedelta(days=100)).timestamp())
        info_text = "Няма активности в базата → дърпам последните 100 дни."

    st.write(info_text)

    activities = fetch_activities_since(access_token, after_ts)
    st.write(f"Намерени активности от Strava: {len(activities)}")

    new_acts = 0
    total_stream_rows = 0

    for act in activities:
        try:
            local_id = upsert_activity(act)
            new_acts += 1
        except Exception:
            continue

        try:
            streams = fetch_activity_streams(access_token, act["id"])
            total_stream_rows += save_streams(local_id, streams)
            time.sleep(0.3)  # малко забавяне за rate limit
        except Exception:
            continue

    return new_acts, total_stream_rows


# --------------------------
# Streamlit UI
# --------------------------
st.title("onFlows – Strava → Supabase sync (raw data)")

st.markdown(
    """
Този app прави следното:

1. Взима токен от Strava чрез *refresh_token*  
2. Намира всички нови активности (последни 30 дни или след последната в базата)  
3. Записва:
   - мета-информацията в `activities`
   - суровите стриймове в `activity_streams`
"""
)

if st.button("Синхронизирай с Strava"):
    with st.spinner("Синхронизирам..."):
        try:
            new_acts, total_rows = sync_from_strava()
            st.success(
                f"Готово! Нови/обновени активности: {new_acts}, stream редове: {total_rows}"
            )
        except Exception as e:
            st.error(f"Неочаквана грешка при sync_from_strava: {e}")

st.subheader("Последни активности в базата")

try:
    res = (
        supabase.table("activities")
        .select("*")
        .order("start_date", desc=True)
        .limit(10)
        .execute()
    )
    if res.data:
        st.dataframe(res.data)
    else:
        st.info("Все още няма записани активности.")
except Exception as e:
    st.warning(f"Не успях да заредя активности от Supabase: {e}")

