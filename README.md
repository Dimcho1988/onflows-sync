# onFlows Strava — Streamlit-only (Browser) Starter

This starter runs entirely as a Streamlit app (no separate backend). It:
- Completes Strava OAuth using your app's public URL as the redirect URI.
- Stores tokens and data in Supabase.
- Polls Strava for recent activities (no webhooks).
- Fetches activity streams and does light validation/filtering before insert.

## One-time setup

1) Create a Strava API application:
   - Authorization Callback Domain: your Streamlit app domain (e.g., `onflows-sync.streamlit.app`).
   - Redirect URI: `https://<your-app>.streamlit.app` (same as APP_BASE_URL). Strava will append `?code=...`.

2) Create Supabase tables (simple baseline schema):

```sql
-- athletes (optional, denormalized cache)
create table if not exists athletes (
  athlete_id bigint primary key,
  firstname text,
  lastname text,
  profile text,
  created_at timestamp with time zone default now()
);

-- oauth_tokens (per athlete)
create table if not exists oauth_tokens (
  athlete_id bigint primary key,
  access_token text not null,
  refresh_token text not null,
  expires_at bigint not null,
  scope text,
  updated_at timestamp with time zone default now()
);

-- activities metadata
create table if not exists activities (
  activity_id bigint primary key,
  athlete_id bigint not null,
  sport_type text,
  start_date_utc timestamptz,
  start_date_local timestamptz,
  elapsed_time_s integer,
  moving_time_s integer,
  distance_m double precision,
  avg_speed_ms double precision,
  avg_hr_bpm double precision,
  name text,
  ingest_status text,
  created_at timestamptz default now()
);

-- raw_streams (1 Hz rows; you can shard/partition later)
create table if not exists raw_streams (
  activity_id bigint,
  ts_rel_s integer,
  dist_m double precision,
  speed_ms double precision,
  hr_bpm double precision,
  altitude_m double precision,
  cadence_spm double precision,
  lat double precision,
  lon double precision
);
create index if not exists raw_streams_activity_idx on raw_streams(activity_id, ts_rel_s);
```

3) Streamlit Cloud secrets: go to **Settings → Secrets** and paste:

```toml
# Strava
STRAVA_CLIENT_ID = "your_id"
STRAVA_CLIENT_SECRET = "your_secret"

# App
APP_BASE_URL = "https://<your-app>.streamlit.app"  # must match Strava Redirect URI

# Supabase
SUPABASE_URL = "https://<project>.supabase.co"
SUPABASE_SERVICE_KEY = "service_role_key"          # server-side; keep secret!
```

## Deploy

- Push these files to a GitHub repo and deploy on Streamlit Community Cloud.
- Set the app URL in Strava as the Redirect URI and in `APP_BASE_URL`.

## Usage

- Open the app → click **Connect with Strava**.
- After consent, you’re redirected back with `?code=...` → app exchanges the code, stores tokens, and runs an initial sync (recent 30 days).
- Click **Sync latest** any time to poll new activities and ingest streams.

## Notes

- This starter avoids Strava webhooks to keep everything "browser-only".
- For production, add RLS policies, encryption for tokens, and rate limiting; consider moving streams to a partitioned table.
