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
SUPABASE_KEY = st.secrets["supabase"]["service_role_key"]  # anon или service_role

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ==========================================================
# 1) OAuth callback + зареждане на текущия потребител
# ==========================================================
def load_current_user():
    """
    Връща dict с текущия Strava user (от strava_users),
    ако има такъв за тази сесия.
    """
    # 1) Ако току-що сме върнати от Strava с ?code=
    query_params = st.experimental_get_query_params()
    if "code" in query_params:
        auth_code = query_params["code"][0]
        st.info(f"Получен code от Strava: {auth_code}")

        # обменяме code -> токени
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
        access_token = token_info["access_token"]
        refresh_token = token_info["refresh_token"]
        scope = token_info.get("scope")

        # взимаме инфо за атлета
        athlete_resp = requests.get(
            "https://www.strava.com/api/v3/athlete",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if not athlete_resp.ok:
            st.error(
                f"Грешка при /athlete: "
                f"{athlete_resp.status_code} {athlete_resp.text}"
            )
            return None
        athlete = athlete_resp.json()
        athlete_id = athlete["id"]
        name = (
            athlete.get("username")
            or f'{athlete.get("firstname","")} {athlete.get("lastname","")}'.strip()
            or f"Athlete {athlete_id}"
        )

        # запис/ъпдейт в strava_users
        try:
            res = (
                supabase.table("strava_users")
                .upsert(
                    {
                        "strava_athlete_id": athlete_id,
                        "name": name,
                        "refresh_token": refresh_token,
                    },
                    on_conflict="strava_athlete_id",
                )
                .execute()
            )
            user_row = res.data[0]
            st.success(
                f"Свързан си със Strava като: {name} "
                f"(athlete_id={athlete_id}), scope={scope}"
            )
        except Exception as e:
            st.error(f"Supabase грешка при запис в strava_users: {e}")
            return None

        # помним го в session_state
        st.session_state["current_athlete_id"] = athlete_id
        return user_row

    # 2) Ако вече имаме athlete_id в session_state -> зареждаме от базата
    athlete_id = st.session_state.get("current_athlete_id")
    if athlete_id:
        try:
            res = (
                supabase.table("strava_users")
                .select("*")
                .eq("strava_athlete_id", athlete_id)
                .limit(1)
                .execute()
            )
            data = res.data
            if data:
                return data[0]
        except Exception as e:
            st.error(f"Supabase грешка при зареждане на текущия user: {e}")
            return None

    return None


# ==========================================================
# 2) STRAVA helper-и
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
# 3) SUPABASE helper-и
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
        "user_id": user_id,
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
def sync_from_strava(user_row: dict):
    user_id = user_row["id"]               # локално id в strava_users
    refresh_token = user_row["refresh_token"]
    name = user_row.get("name") or f"user {user_id}"

    st.write(f"Синхронирам Strava потребител: {name} (local id={user_id})")

    access_token = get_strava_access_token(refresh_token)

    last_dt = get_last_activity_start_date(user_id)
    if last_dt:
        after_ts = int(last_dt.timestamp()) - 60
        info_text = (
            f"Синхронирам от последната записана активност за този юзер: {last_dt}."
        )
    else:
        after_ts = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp())
        info_text = "Няма активности в базата за този юзер → дърпам последните 365 дни."

    st.write(info_text)

    activities = fetch_activities_since(access_token, after_ts)
    st.write(f"Намерени активности от Strava: {len(activities)}")

    new_acts = 0
    total_stream_rows = 0

    for act in activities:
        try:
            local_id = upsert_activity(act, user_id=user_id)
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

# Зареждаме текущия потребител (ако има такъв)
current_user = load_current_user()

# Ако нямаме свързан Strava акаунт → показваме бутон за свързване
if not current_user:
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

# Ако има свързан user:
st.success(
    f"Свързан си със Strava като: {current_user.get('name')} "
    f"(athlete_id={current_user.get('strava_athlete_id')})"
)

if st.button("Синхронизирай моите Strava активности"):
    with st.spinner("Синхронизирам..."):
        try:
            new_acts, total_rows = sync_from_strava(current_user)
            st.success(
                f"Готово! Нови/обновени активности: {new_acts}, stream редове: {total_rows}"
            )
        except Exception as e:
            st.error(f"Неочаквана грешка при sync_from_strava: {e}")

st.subheader("Последни мои активности в базата")

try:
    res = (
        supabase.table("activities")
        .select("*")
        .eq("user_id", current_user["id"])
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
