import math

DEFAULTS = {
    "hr_min": 40,
    "hr_max": 230,
    "min_speed_ms": 0.2,
    "zero_speed_pause_sec": 30,
    "gps_jump_m": 50.0,
    "missing_ratio_warn": 0.2,
}

def haversine_m(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def validate_transform(streams: dict):
    t = streams.get("time", {}).get("data") or []
    dist = streams.get("distance", {}).get("data") or []
    latlng = streams.get("latlng", {}).get("data") or []
    alt = streams.get("altitude", {}).get("data") or []
    vel = streams.get("velocity_smooth", {}).get("data") or []
    hr = streams.get("heartrate", {}).get("data") or []
    cad = streams.get("cadence", {}).get("data") or []
    rows = []
    for i in range(len(t)):
        hr_v = hr[i] if i < len(hr) else None
        spd = vel[i] if i < len(vel) else None
        if hr_v is not None and (hr_v < DEFAULTS["hr_min"] or hr_v > DEFAULTS["hr_max"]):
            hr_v = None
        if spd is not None and spd < DEFAULTS["min_speed_ms"]:
            spd = 0.0
        lat = lon = None
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

def detect_artifacts(rows):
    arts = []
    if not rows:
        return arts
    total = len(rows)
    missing = 0
    for r in rows:
        if r["speed_ms"] is None and r["hr_bpm"] is None and r["altitude_m"] is None:
            missing += 1
    miss_ratio = missing / max(1, total)
    if miss_ratio >= DEFAULTS["missing_ratio_warn"]:
        arts.append({
            "ts_rel_s_from": rows[0]["ts_rel_s"],
            "ts_rel_s_to": rows[-1]["ts_rel_s"],
            "kind": "missing_data_ratio",
            "severity": 2,
            "note": f"missing_ratio={miss_ratio:.2f}"
        })
    thr = DEFAULTS["zero_speed_pause_sec"]
    run_len = 0
    start_ts = None
    for r in rows:
        spd = r["speed_ms"]
        if spd is not None and spd == 0.0:
            run_len += 1
            if start_ts is None:
                start_ts = r["ts_rel_s"]
        else:
            if run_len >= thr:
                arts.append({
                    "ts_rel_s_from": start_ts,
                    "ts_rel_s_to": start_ts + run_len - 1,
                    "kind": "zero_speed_pause",
                    "severity": 1,
                    "note": f"duration={run_len}s"
                })
            run_len = 0
            start_ts = None
    if run_len >= thr:
        arts.append({
            "ts_rel_s_from": start_ts,
            "ts_rel_s_to": start_ts + run_len - 1,
            "kind": "zero_speed_pause",
            "severity": 1,
            "note": f"duration={run_len}s"
        })
    for i in range(1, len(rows)):
        a, b = rows[i - 1], rows[i]
        def hv(a, b):
            if None in (a["lat"], a["lon"], b["lat"], b["lon"]):
                return None
            return haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
        d = hv(a, b)
        if d is not None and d > DEFAULTS["gps_jump_m"]:
            arts.append({
                "ts_rel_s_from": b["ts_rel_s"] - 1,
                "ts_rel_s_to": b["ts_rel_s"],
                "kind": "gps_jump",
                "severity": 2,
                "note": f"jump_m={d:.1f}"
            })
    return arts
