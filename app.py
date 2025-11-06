
import os, math, time
import streamlit as st
import httpx, pandas as pd, numpy as np
from urllib.parse import urlencode
from supabase import create_client

APP_BASE_URL = os.getenv("APP_BASE_URL","").rstrip("/")
STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", f"{APP_BASE_URL}/?oauth_redirect=1")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

@st.cache_resource(show_spinner=False)
def sb_public():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

@st.cache_resource(show_spinner=False)
def sb_service():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def strava_auth_url():
    params = {"client_id": STRAVA_CLIENT_ID,"response_type":"code","redirect_uri":OAUTH_REDIRECT_URI,"approval_prompt":"auto","scope":"read,activity:read_all"}
    return "https://www.strava.com/oauth/authorize?" + urlencode(params)

def token_from_code(code):
    r = httpx.post("https://www.strava.com/oauth/token", data={"client_id":STRAVA_CLIENT_ID,"client_secret":STRAVA_CLIENT_SECRET,"code":code,"grant_type":"authorization_code"}, timeout=30)
    r.raise_for_status(); return r.json()

def refresh_if_needed(tok):
    now = int(time.time())
    if tok["expires_at"] - now > 60: return tok
    r = httpx.post("https://www.strava.com/oauth/token", data={"client_id":STRAVA_CLIENT_ID,"client_secret":STRAVA_CLIENT_SECRET,"grant_type":"refresh_token","refresh_token":tok["refresh_token"]}, timeout=30)
    r.raise_for_status(); t=r.json(); tok.update(t); return tok

def import_last_n(token, n=30, user_id=""):
    sbs = sb_service()
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    with httpx.Client(timeout=60) as c:
        r = c.get("https://www.strava.com/api/v3/athlete/activities", headers=headers, params={"per_page":n,"page":1}); r.raise_for_status(); acts=r.json()
        for a in acts:
            sbs.table("activities").upsert({
                "activity_id": a["id"],
                "user_id": user_id,
                "sport_type": a.get("sport_type","Run"),
                "start_utc": a["start_date"],
                "start_local": a["start_date_local"],
                "elapsed_time_s": a.get("elapsed_time"),
                "moving_time_s": a.get("moving_time"),
                "distance_m": a.get("distance"),
                "elev_gain_m": a.get("total_elevation_gain"),
                "avg_speed_ms": a.get("average_speed"),
                "max_speed_ms": a.get("max_speed"),
                "avg_hr_bpm": a.get("average_heartrate"),
                "max_hr_bpm": a.get("max_heartrate"),
                "device": (a.get("device_name") or ""),
                "gear_id": a.get("gear_id")
            }).execute()
            s = c.get(f"https://www.strava.com/api/v3/activities/{a['id']}/streams", headers=headers, params={"keys":"time,latlng,distance,altitude,heartrate,cadence,watts,velocity_smooth,grade_smooth","key_by_type":"true"}); s.raise_for_status()
            streams = s.json()
            time_s = streams.get("time",{}).get("data",[])
            if not time_s: continue
            df = pd.DataFrame({"ts_rel_s": time_s})
            def pull(key, col):
                v = streams.get(key,{}).get("data",[])
                if key=="latlng" and v:
                    df["lat"] = [p[0] if p else None for p in v]; df["lng"] = [p[1] if p else None for p in v]
                elif v: df[col] = pd.Series(v, dtype="float")
                else: df[col] = np.nan
            pull("latlng",""); pull("distance","dist_m"); pull("altitude","alt_m"); pull("heartrate","hr_bpm"); pull("cadence","cad_rpm"); pull("watts","watts"); pull("velocity_smooth","speed_ms"); pull("grade_smooth","grade")
            v_low, hr_work, L_low = 0.6, 95, 10
            df["moving"] = df["speed_ms"].fillna(0) > v_low
            roll = df["speed_ms"].fillna(0).rolling(L_low, min_periods=1).min()
            df["low_speed_hr_inconsistent"] = ((roll <= v_low) & (df["hr_bpm"].fillna(0) >= hr_work))
            df["activity_id"] = a["id"]
            cols = ["activity_id","ts_rel_s","lat","lng","dist_m","alt_m","speed_ms","hr_bpm","cad_rpm","watts","grade","moving","low_speed_hr_inconsistent"]
            df = df[cols].rename(columns={"low_speed_hr_inconsistent":"flags"}); df["flags"] = df["flags"].apply(lambda x: {"low_speed_hr_inconsistent": bool(x)})
            chunk=500
            for i in range(0,len(df),chunk):
                sbs.table("raw_streams").upsert(df.iloc[i:i+chunk].to_dict(orient="records")).execute()
            # 30s windows
            W=30; total_t = int(df["ts_rel_s"].max()) + 1; start = pd.Timestamp(a["start_date"], tz="UTC"); recs=[]
            for k in range(0, math.ceil(total_t/W)):
                lo, hi = k*W, min((k+1)*W, total_t); seg = df[(df["ts_rel_s"]>=lo)&(df["ts_rel_s"]<hi)]
                if seg.empty: continue
                max_gap = int(seg["speed_ms"].isna().astype(int).groupby((seg["speed_ms"].notna()).cumsum()).sum().max() or 0)
                q = (seg["speed_ms"].notna().mean()*0.4) + ((1.0 - max_gap/W)*0.2) + (seg["hr_bpm"].notna().mean()*0.2) + ((1.0 - seg["flags"].apply(lambda x: x.get("low_speed_hr_inconsistent", False)).mean())*0.2)
                q = max(0.0, min(1.0, float(q)))
                recs.append({"activity_id":a["id"],"window_s":W,"win_idx":k,"t_start":(start + pd.Timedelta(seconds=lo)).isoformat(),"t_end":(start + pd.Timedelta(seconds=hi)).isoformat(),"mean_speed_ms":float(seg["speed_ms"].mean(skipna=True)),"p95_speed_ms":float(seg["speed_ms"].quantile(0.95)),"median_hr_bpm":float(seg["hr_bpm"].median(skipna=True)) if "hr_bpm" in seg else None,"hr_valid_fraction":float(seg["hr_bpm"].notna().mean()) if "hr_bpm" in seg else 0.0,"mean_grade":float(seg["grade"].mean(skipna=True)) if "grade" in seg else None,"elev_delta_m":float(seg["alt_m"].dropna().diff().sum()) if "alt_m" in seg else None,"distance_delta_m":float(seg["dist_m"].dropna().diff().sum()) if "dist_m" in seg else None,"lat_center":float(seg["lat"].mean(skipna=True)) if "lat" in seg else None,"lng_center":float(seg["lng"].mean(skipna=True)) if "lng" in seg else None,"moving_fraction":float(seg["moving"].mean()),"n_points":int(len(seg)),"gap_seconds":int(max_gap),"q_score":q})
            for i in range(0,len(recs),500):
                sb_service().table("agg_windows").upsert(recs[i:i+500]).execute()

def main():
    st.set_page_config(page_title="onFlows · Browser-only", layout="centered")
    st.title("onFlows · Strava → Supabase (browser-only)")

    sbp = sb_public()

    with st.expander("Вход в Supabase (magic link)"):
        email = st.text_input("Email за вход")
        if st.button("Изпрати линк"):
            sbp.auth.sign_in_with_otp({"email": email, "options": {"shouldCreateUser": True}})
            st.success("Изпратен е линк.")

    user = sbp.auth.get_user().user if sbp.auth.get_user() else None
    st.write("Потребител:", user.email if user else "не влязъл")

    params = st.query_params
    if params.get("code") and params.get("oauth_redirect") == "1":
        try:
            st.session_state["strava_token"] = token_from_code(params.get("code"))
            st.success("Strava е свързана.")
            st.query_params.clear()
        except Exception as e:
            st.error(str(e))

    st.link_button("Свържи Strava", strava_auth_url())

    if user and "strava_token" in st.session_state:
        if st.button("Синхронизирай последните 30 активности"):
            t = refresh_if_needed(st.session_state["strava_token"])
            import_last_n(t, 30, user_id=user.id)
            st.success("Готово.")
    elif user:
        st.info("Първо свържи Strava.")
    else:
        st.info("Влез чрез magic link.")

    if user:
        data = sbp.table("activities").select("*").eq("user_id", user.id).order("start_local", desc=True).limit(10).execute().data or []
        st.dataframe(data, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
