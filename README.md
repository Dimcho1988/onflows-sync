# onFlows Strava — Streamlit-only v2
Adds:
- Default artifact detection (HR outliers, zero-speed pauses >30s, GPS jumps >50m in 1s, missing-data >20%),
- Calls Supabase function `rebuild_agg_30s(activity_id)` after ingest,
- Buttons to rebuild aggregates for all activities.

## Deploy steps
1) Ensure v1 + v2 schema are applied (you already did).
2) Deploy this repo to Streamlit Cloud.
3) Fill **Settings → Secrets**:
```
STRAVA_CLIENT_ID="..."
STRAVA_CLIENT_SECRET="..."
APP_BASE_URL="https://<your-app>.streamlit.app"
SUPABASE_URL="https://<proj>.supabase.co"
SUPABASE_SERVICE_KEY="service-role-key"
```
4) Connect with Strava from the app.

## Notes
- Artifacts are saved into `stream_artifacts` with basic severities (1..5).
- You can tune thresholds inside `lib/etl.py -> DEFAULTS`.
