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

SUPABASE_URL = st.secrets["supabase"]["url"]
SUPABASE_KEY = st.secrets["supabase"]["service_role_key"]  # може и anon, но service_role е по-удобно за backend app

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ==========================================================
# 1) OAuth: code -> token_info, пазим в session_state
# ==========================================================
def exchange_code_for_tokens(auth_code: str) -> dict | None:
    """Разменя authorization code за token_info (access + refresh + athlete)."""
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

    if not resp.ok:
        st.error("Грешка при обмен на code за токени.")
        return None

    token_info = resp.json()
    # по новия Strava OAuth, инфото за атлета е в отделен /athlete,
    # но ти вече го дърпаше – можем да го вземем тук:
    athlete_resp = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {token_info['access_token']}"},
        timeout=10,
    )
    if not athlete_resp.ok:
        st.error(
            f"Грешка при /athlete: "
            f"{athlete_resp.status_code} {athlete_resp.text}"
        )
        return None
    token_info["athlete"] = athlete_resp.json()
    return token_info


def get_current_token_info() -> dict | None:
    """
    Връща token_info за текущия потребител от session_state,
    или го създава, ако имаме ?code=... в URL-а.
    """
    # ако вече има token_info в сесията -> ползваме него
    if "token_info" in st.session_state:
        return st.session_state["token_info"]

    # ако сме върнати от Strava с ?code=
    query_params = st.experimental_get_query_params()
    if "code" in query_params:
        auth_code = query_params["code"][0]
        st.info(f"Получен code от Strava: {auth_code}")
        token_info = exchange_code_for_tokens(auth_code)
        if token_info:
            st.session_state["token_info"] = token_info
            return token_info

    return None


# ==========================================================
# 2) STRAVA helper-и (с refresh_token от token_info)
# ==========================================================
def get_strava_access_token(refresh_token: str) -> str:
    """Взима нов access_token от Strava чрез подаден refresh_token."""
    token_url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
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
    return r.json()


# ==========================================================
# 3) SUPABASE helper-и (user_id = Strava athlete_id)
# ==========================================================
def get_last_activity_start_date(user_id: int) -> datetime | None:
    """Последната start_date за даден user_id от activities (ако има такава)."""
    try:
        res = (
            supabase.table("activities")
            .select("start_date")
            .eq("user_id", user_id)
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


def upsert_activity(act: dict, user_id: int) -> int:
    """Записва/обновява активност и връща локалното id в activities."""
    row = {
        "user_id": user_id,  # тук ползваме Strava athlete_id
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


# ==========================================================
# 4) Sync pipeline за текущия потребител
# ==========================================================
def sync_from_strava(token_info: dict):
    athlete = token_info["athlete"]
    athlete_id = athlete["id"]          # това ще е user_id в activities
    name = (
        athlete.get("username")
        or f'{athlete.get("firstname","")} {athlete.get("lastname","")}'.strip()
        or f"Athlete {athlete_id}"
    )

    st.write(f"Синхронирам Strava потребител: {name} (athlete_id={athlete_id})")

    refresh_token = token_info["refresh_token"]
    access_token = get_strava_access_token(refresh_token)

    last_dt = get_last_activity_start_date(athlete_id)
    if last_dt:
        after_ts = int(last_dt.timestamp()) - 60
        info_text = (
            f"Синхронирам от последната записана активност за този юзер: {last_dt}."
        )
    else:
        after_ts = int((datetime.now(timezone.utc) - timedelta(days=100)).timestamp())
        info_text = "Няма активности в базата за този юзер → дърпам последните 100 дни."

    st.write(info_text)

    activities = fetch_activities_since(access_token, after_ts)
    st.write(f"Намерени активности от Strava: {len(activities)}")

    new_acts = 0
    total_stream_rows = 0

    for act in activities:
        try:
            local_id = upsert_activity(act, user_id=athlete_id)
            new_acts += 1
        except Exception:
            continue

        try:
            streams = fetch_activity_streams(access_token, act["id"])
            total_stream_rows += save_streams(local_id, streams)
            time.sleep(0.3)
        except Exception:
            continue

    return new_acts, total_stream_rows


# ==========================================================
# 5) Streamlit UI
# ==========================================================
st.title("onFlows – Strava → Supabase sync (raw data)")

st.markdown(
    """
Този app прави следното:

1. Свързва твоя Strava акаунт чрез OAuth (*read, activity:read_all*)  
2. Намира всички нови активности (последни 365 дни или след последната в базата за теб)  
3. Записва:
   - мета-информацията в `activities`
   - суровите стриймове в `activity_streams`
"""
)

token_info = get_current_token_info()

if not token_info:
    # няма токени -> показваме линк за авторизация
    auth_url = (
        "https://www.strava.com/oauth/authorize"
        f"?client_id={STRAVA_CLIENT_ID}"
        "&response_type=code"
        "&redirect_uri=https://onflows-sync-2.streamlit.app"
        "&approval_prompt=force"
        "&scope=read,activity:read_all"
    )
    st.warning("Все още не си свързал Strava акаунт с приложението.")
    st.markdown(
        f"[Свържи се със Strava]({auth_url})  ← натисни, логни се в Strava и Authorize"
    )
    st.stop()

athlete = token_info["athlete"]
st.success(
    f"Свързан си със Strava като: "
    f"{athlete.get('username') or athlete.get('firstname')} {athlete.get('lastname')}"
    f" (athlete_id={athlete.get('id')})"
)

if st.button("Синхронизирай моите Strava активности"):
    with st.spinner("Синхронизирам..."):
        try:
            new_acts, total_rows = sync_from_strava(token_info)
            st.success(
                f"Готово! Нови/обновени активности: {new_acts}, stream редове: {total_rows}"
            )
        except Exception as e:
            st.error(f"Неочаквана грешка при sync_from_strava: {e}")

st.subheader("Последни мои активности в базата")

try:
    athlete_id = athlete["id"]
    res = (
        supabase.table("activities")
        .select("*")
        .eq("user_id", athlete_id)
        .order("start_date", desc=True)
        .limit(20)
        .execute()
    )
    if res.data:
        st.dataframe(res.data)
    else:
        st.info("Все още няма записани активности за този потребител.")
except Exception as e:
    st.warning(f"Не успях да заредя активности от Supabase: {e}")
