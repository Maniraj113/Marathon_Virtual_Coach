import json
import logging
import re
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

def extract_json(raw: str) -> Dict[str, Any]:
    """Parse JSON from LLM output, stripping markdown fences if present."""
    try:
        cleaned = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned)
    except Exception as e:
        logger.warning(f"JSON parse failed: {e}. Raw snippet: {raw[:300]}")
        return {"error": "json_parse_failed", "raw": raw}

def to_table(data_list, headers: list) -> str:
    """Converts a list of dicts to a Markdown table."""
    if not data_list or data_list == "MISSING":
        return "MISSING"
    h_str = "| " + " | ".join(headers) + " |"
    sep   = "| " + " | ".join(["---"] * len(headers)) + " |"
    rows = []
    for item in data_list:
        row = "| " + " | ".join(
            str(item.get(h.lower().replace(" ", "_"), "N/A")) for h in headers
        ) + " |"
        rows.append(row)
    return "\n".join([h_str, sep] + rows)

def extract_personal_details(message: str) -> Dict[str, str]:
    """Extracts weight, age, height from natural language message."""
    details = {}
    if not message:
        return details
        
    # Heuristic for Weight, Age, Height
    w = re.search(r"(\d+(?:\.\d+)?)\s*(kg|lbs|pounds)", message, re.I)
    a = re.search(r"(\d+)\s*(?:years old|year old|yo)", message, re.I)
    h = re.search(r"(\d+(?:\.\d+)?)\s*(cm|m|feet|ft|inches|in)", message, re.I)
    
    if w: details["weight"] = f"{w.group(1)} {w.group(2)}"
    if a: details["age"] = a.group(1)
    if h: details["height"] = f"{h.group(1)} {h.group(2)}"
    
    return details

def calculate_intensity_score(avg_hr: float, max_hr: float, age: int = 30) -> str:
    """Calculates relative intensity based on Max HR (estimated or actual)."""
    estimated_max = 220 - age
    effective_max = max(max_hr, estimated_max)
    ratio = avg_hr / effective_max
    
    if ratio > 0.85: return "Very High (Threshold/Anaerobic)"
    if ratio > 0.75: return "High (Tempo/Brisk)"
    if ratio > 0.65: return "Moderate (Aerobic/Base)"
    return "Low (Recovery)"

def detect_performance_drift(current_pace: str, historical_avg_pace: str) -> str:
    """Compares current pace vs history to detect progress or fatigue."""
    try:
        # Simplified parser for Pace format 'MM:SS'
        def to_sec(p):
            m, s = map(int, p.split(':'))
            return m * 60 + s
        
        curr = to_sec(current_pace.split('/')[0]) # Handling '5:30/km'
        hist = to_sec(historical_avg_pace.split('/')[0])
        
        diff = hist - curr # Negative = faster today
        if diff > 15: return "Significant progress! You are much faster than your average."
        if diff < -15: return "Noticeable drift. You are slower today; potentially fatigue or heat."
        return "Consistent with your historical baseline."
    except:
        return "Baseline comparison unavailable."

def build_analyst_prompt(analysis_data: Dict[str, Any], activity_type: str, memory_context: str = "No previous memory facts available.") -> str:
    """
    Constructs the structured text prompt for the Coaching LLM based on activity data.
    """
    p_or_s = "Pace" if activity_type.lower() in ["run", "walk", "hike", "training", "race"] else "Speed"
    laps_table   = to_table(analysis_data.get("laps"),   ["Lap", "Distance", "Time", p_or_s, "Elev", "HR"])
    splits_table = to_table(analysis_data.get("splits"), ["Km",  "Time",     p_or_s, "HR",   "Elev"])

    prompt = (
        f"ATHLETE: {analysis_data['athlete_name']}\n"
        f"ACTIVITY: {analysis_data['activity_name']} "
        f"(Strava Type: {analysis_data.get('activity_type', 'Run')} | "
        f"Session Type: {activity_type.upper()})\n"
        f"DISTANCE: {analysis_data['total_distance_km']} km | "
        f"TIME: {analysis_data['total_time']} | "
        f"AVG {p_or_s.upper()}: {analysis_data['avg_pace_overall']}{'' if 'km/h' in analysis_data['avg_pace_overall'] else '/km'}\n"
        f"ELEVATION GAIN: {analysis_data.get('total_elevation_m', 0)} m\n"
        f"DESCRIPTION: {analysis_data.get('description', 'None')}\n"
        f"--- LAP DATA (Source of Truth for structured workouts) ---\n"
        f"{laps_table}\n\n"
        f"--- KM SPLITS (Auto 1km markers) ---\n"
        f"{splits_table}\n\n"
        f"HEART RATE:\n"
        f"  Avg: {analysis_data['avg_hr']} | Max: {analysis_data['max_hr']}\n\n"
        f"ATHLETE PROFILE:\n"
        f"  Age: {analysis_data.get('age') if analysis_data.get('age') else 'UNKNOWN'}\n"
        f"  Yearly goal: {analysis_data.get('yearly_goal') if analysis_data.get('yearly_goal') else 'UNKNOWN'}\n"
        f"  Activity preference: {analysis_data.get('activity_preference') if analysis_data.get('activity_preference') else 'UNKNOWN'}\n\n"
        f"ATHLETE PERSONAL DETAILS (if provided):\n"
        f"  {analysis_data.get('personal_details_str', 'None provided yet.')}\n\n"
        f"ATHLETE LONG-TERM CONTEXT (Memory Bank):\n"
        f"  {memory_context}\n\n"
        f"COACHING TASK:\n"
        f"  Session type is '{activity_type}' (training run vs race - adjust expectations accordingly).\n"
        f"  1. Use EXACT values from tables. Do not round or guess.\n"
        f"  2. If any metric shows MISSING, state it upfront. Do not assume.\n"
        f"  3. Balanced Feedback: Praise the effort but warn about high HR if disproportionate.\n"
        f"  4. Format everything as brief bullet points. Avoid long paragraphs.\n"
        f"  5. Use the exact markdown structure defined in your system prompt."
    )
    return prompt
