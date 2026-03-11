"""
Coach Tools — Strava Data Fetcher & Cleaner
============================================
Fetches raw Strava data (activity details + streams),
logs the raw response, cleans/structures it, then logs
the cleaned payload. This is the DataFetcherAgent layer.

Data flow:
  refresh_token + activity_id
     → Strava Auth
     → [LOG raw data]
     → Clean & structure (laps, splits, streams)
     → [LOG clean payload]
     → Return clean payload to CoachingPipeline
"""

import logging
import json
import requests
import statistics
from typing import Dict, Any, List

from services.strava_service import StravaService

logger = logging.getLogger(__name__)
REST_LAP_THRESHOLD_M = 0  # Analyze everything

# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_pace(moving_time_s: float, distance_m: float) -> str:
    """Convert (seconds, metres) → pace string 'M:SS/km'."""
    if not distance_m or distance_m == 0:
        return "0:00"
    pace_min_km = (moving_time_s / (distance_m / 1000)) / 60
    mins = int(pace_min_km)
    secs = int((pace_min_km - mins) * 60)
    return f"{mins}:{secs:02d}"


def _fmt_time(seconds: float) -> str:
    """Convert total seconds → 'H:MM:SS' or 'M:SS'."""
    if not seconds:
        return "0:00"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}:{m:02d}:{s:02d}" if h > 0 else f"{m}:{s:02d}"


def _fmt_elev(diff: float) -> str:
    """Format signed elevation difference: '+1m' or '-2m'."""
    return f"{diff:+.0f}m"


# ── Main Tool ─────────────────────────────────────────────────────────────────

def analyze_activity_deep(
    activity_id: int,
    refresh_token: str,
    athlete_name: str,
) -> Dict[str, Any]:
    """
    DataFetcherAgent + DataAnalystAgent combined.

    1. Authenticates with Strava
    2. Fetches raw activity details + streams
    3. Logs raw data (Observe)
    4. Cleans & structures the data (Think)
    5. Logs clean payload
    6. Returns clean payload to CoachingPipeline (Act)
    """
    logger.info(f"[DataFetcher] Activity={activity_id} | Athlete={athlete_name}")

    if not refresh_token:
        logger.error("[DataFetcher] No refresh token — cannot authenticate")
        return {"error": "Strava refresh token not provided."}

    # ── Auth ──────────────────────────────────────────────────────────────────
    strava = StravaService(refresh_token=refresh_token)
    if not strava.refresh_access_token():
        return {"error": "Strava authentication failed. Check refresh token."}

    headers = {"Authorization": f"Bearer {strava.access_token}"}

    # ── Fetch raw activity details ────────────────────────────────────────────
    resp = requests.get(
        f"https://www.strava.com/api/v3/activities/{activity_id}",
        headers=headers,
    )
    if resp.status_code != 200:
        return {"error": f"Strava activity fetch failed: HTTP {resp.status_code} — {resp.text[:200]}"}

    raw_details = resp.json()

    # ── LOG RAW DATA ──────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("[RAW] Strava Activity Details")
    logger.info(f"  name       : {raw_details.get('name')}")
    logger.info(f"  type       : {raw_details.get('type')}")
    logger.info(f"  distance   : {raw_details.get('distance')} m")
    logger.info(f"  moving_time: {raw_details.get('moving_time')} s")
    logger.info(f"  has_hr     : {raw_details.get('has_heartrate')}")
    logger.info(f"  avg_hr     : {raw_details.get('average_heartrate')}")
    logger.info(f"  max_hr     : {raw_details.get('max_heartrate')}")
    logger.info(f"  laps count : {len(raw_details.get('laps', []))}")
    logger.info(f"  splits count: {len(raw_details.get('splits_metric', []))}")
    logger.info("=" * 60)

    # ── Clean Laps ────────────────────────────────────────────────────────────
    # Matches Strava's own lap table: Lap | Distance | Time | Pace | Elev | HR
    raw_laps = raw_details.get("laps", [])
    cleaned_laps = []
    for lap in raw_laps:
        dist_m = lap.get("distance", 0)
        mov_t  = lap.get("moving_time", 0)
        hr_val = lap.get("average_heartrate")
        elev_diff = lap.get("elevation_difference", 0)   # signed ± value

        cleaned_laps.append({
            "lap":      lap.get("lap_index"),
            "distance": f"{dist_m/1000:.2f} km",
            "time":     _fmt_time(mov_t),
            "pace":     _fmt_pace(mov_t, dist_m),
            "elev":     _fmt_elev(elev_diff),
            "hr":       f"{round(hr_val)} bpm" if hr_val else "N/A",
        })

    # ── Clean Splits (auto 1km markers) ───────────────────────────────────────
    cleaned_splits = []
    for s in raw_details.get("splits_metric", []):
        dist_m = s.get("distance", 0)
        mov_t  = s.get("moving_time", 0)
        hr_val = s.get("average_heartrate")
        elev_diff = s.get("elevation_difference", 0)

        cleaned_splits.append({
            "km":   s.get("split"),
            "time": _fmt_time(mov_t),
            "pace": _fmt_pace(mov_t, dist_m),
            "hr":   f"{round(hr_val)} bpm" if hr_val else "N/A",
            "elev": _fmt_elev(elev_diff),
        })

    # ── Pace Consistency (stream-based) ───────────────────────────────────────
    # Removed stream logic for simplicity

    # ── HR Metrics ────────────────────────────────────────────────────────────
    has_hr  = raw_details.get("has_heartrate", False)
    avg_hr  = round(raw_details["average_heartrate"], 1) if has_hr and raw_details.get("average_heartrate") else "MISSING"
    max_hr  = raw_details.get("max_heartrate") if has_hr else "MISSING"

    # ── Overall Stats ─────────────────────────────────────────────────────────
    total_dist_km = round(raw_details.get("distance", 0) / 1000, 2)
    moving_secs   = raw_details.get("moving_time", 0)
    overall_pace  = _fmt_pace(moving_secs, raw_details.get("distance", 0))

    # ── Build clean payload ───────────────────────────────────────────────────
    clean_payload = {
        "athlete_name":       athlete_name,
        "activity_id":        activity_id,
        "activity_name":      raw_details.get("name", "Run"),
        "activity_type":      raw_details.get("type", "Run"),
        "has_heartrate":      has_hr,
        "description":        raw_details.get("description") or "None",
        "total_distance_km":  total_dist_km,
        "total_time":         _fmt_time(moving_secs),
        "avg_pace_overall":   overall_pace,
        "total_elevation_m":  raw_details.get("total_elevation_gain", 0),
        "laps":               cleaned_laps if cleaned_laps else "MISSING",
        "splits":             cleaned_splits if cleaned_splits else "MISSING",
        "avg_hr":             avg_hr,
        "max_hr":             max_hr,
    }

    # ── LOG CLEAN PAYLOAD ─────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("[CLEAN] Structured Payload for LLM")
    logger.info(f"  athlete       : {clean_payload['athlete_name']}")
    logger.info(f"  activity      : {clean_payload['activity_name']} ({clean_payload['activity_type']})")
    logger.info(f"  distance      : {clean_payload['total_distance_km']} km")
    logger.info(f"  time          : {clean_payload['total_time']}")
    logger.info(f"  avg_pace      : {clean_payload['avg_pace_overall']}/km")
    logger.info(f"  avg_hr        : {clean_payload['avg_hr']}")
    logger.info(f"  max_hr        : {clean_payload['max_hr']}")
    logger.info(f"  laps count    : {len(cleaned_laps)}")
    logger.info(f"  splits count  : {len(cleaned_splits)}")
    logger.info("=" * 60)

    return clean_payload
