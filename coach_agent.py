"""
Athlete Analyzer — Coaching Pipeline
======================================
Multi-Agent Architecture:

  DataFetcherAgent  (Observe) → BigQuery lookup + Strava fetch + clean data
  DataAnalystAgent  (Think)   → Structure data for LLM, build prompt tables
  CoachAgent        (Act)     → LLM call + token telemetry + stream response

Session: Vertex AI Session Service only (no InMemory fallback).
         user_id = strava_athlete_id (enforced from API layer).
         session.state["last_activity"] stores clean payload for follow-up turns.
"""

import json
import os
import time
import logging
import yaml
import asyncio
from typing import AsyncGenerator, Dict, Any, Optional
from pathlib import Path

from dotenv import load_dotenv
from google.adk.agents import BaseAgent, InvocationContext
from google.adk.events import Event
from google.adk.runners import Runner
from google.adk.sessions.vertex_ai_session_service import VertexAiSessionService
from google.genai import types
from pydantic import BaseModel
from vertexai.generative_models import GenerativeModel

import vertexai
from google import genai

load_dotenv()
logger = logging.getLogger(__name__)

# ── Vertex AI Initialization ──────────────────────────────────────────────────
_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
_location   = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
_model_name = os.getenv("COACH_AGENT_MODEL", "gemini-2.0-flash")

if not _project_id:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT env var is required.")

vertexai.init(project=_project_id, location=_location)

# ── Prompts ───────────────────────────────────────────────────────────────────
_PROMPTS_PATH = Path(__file__).parent / "prompts.yaml"
with open(_PROMPTS_PATH, "r") as f:
    PROMPTS = yaml.safe_load(f)


# ── Pipeline State ────────────────────────────────────────────────────────────
class AthleteState(BaseModel):
    """Structured state passed between pipeline stages."""
    user_id:        str = ""
    session_id:     str = ""
    original_query: str = ""
    intent:         str = ""
    activity_type:  str = ""
    final_response: str = ""


# ── LLM Specialist Caller with Telemetry ─────────────────────────────────────
def _call_specialist(system_instruction: str, user_message: str, label: str = "LLM") -> str:
    """
    CoachAgent — calls Gemini via Vertex AI.
    Logs prompt tokens, candidate tokens, total tokens, and latency.
    """
    client = genai.Client(vertexai=True, project=_project_id, location=_location)
    
    t0 = time.perf_counter()
    response = client.models.generate_content(
        model=_model_name,
        contents=user_message,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction
        )
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)

    # Token telemetry
    usage = getattr(response, "usage_metadata", None)
    if usage:
        prompt_t    = getattr(usage, "prompt_token_count", "n/a")
        candidate_t = getattr(usage, "candidates_token_count", "n/a")
        total_t     = getattr(usage, "total_token_count", "n/a")
        logger.info(
            f"[Telemetry:{label}] model={_model_name} | latency={latency_ms}ms "
            f"| prompt_tokens={prompt_t} | candidate_tokens={candidate_t} | total_tokens={total_t}"
        )
    else:
        logger.info(f"[Telemetry:{label}] model={_model_name} | latency={latency_ms}ms | usage_metadata=unavailable")

    return response.text


def _extract_json(raw: str) -> Dict[str, Any]:
    """Parse JSON from LLM output, stripping markdown fences if present."""
    try:
        cleaned = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned)
    except Exception as e:
        logger.warning(f"JSON parse failed: {e}. Raw snippet: {raw[:300]}")
        return {"error": "json_parse_failed", "raw": raw}


# ── DataAnalystAgent — Prompt Table Builder ───────────────────────────────────
def _build_analyst_prompt(analysis_data: Dict[str, Any], activity_type: str) -> str:
    """
    DataAnalystAgent (Think phase).
    Converts clean payload into a structured text prompt for the CoachAgent LLM.
    Builds Markdown tables from laps and splits.
    """
    def to_table(data_list, headers: list) -> str:
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

    laps_table   = to_table(analysis_data.get("laps"),   ["Lap", "Distance", "Time", "Pace", "Elev", "HR"])
    splits_table = to_table(analysis_data.get("splits"), ["Km",  "Time",     "Pace", "HR",   "Elev"])

    # Cleaned: No consistency string generated.

    prompt = (
        f"ATHLETE: {analysis_data['athlete_name']}\n"
        f"ACTIVITY: {analysis_data['activity_name']} "
        f"(Strava Type: {analysis_data.get('activity_type', 'Run')} | "
        f"Session Type: {activity_type.upper()})\n"
        f"DISTANCE: {analysis_data['total_distance_km']} km | "
        f"TIME: {analysis_data['total_time']} | "
        f"AVG PACE: {analysis_data['avg_pace_overall']}/km\n"
        f"ELEVATION GAIN: {analysis_data.get('total_elevation_m', 0)} m\n"
        f"DESCRIPTION: {analysis_data.get('description', 'None')}\n\n"
        f"--- LAP DATA (Source of Truth for structured workouts) ---\n"
        f"{laps_table}\n\n"
        f"--- KM SPLITS (Auto 1km markers) ---\n"
        f"{splits_table}\n\n"
        f"HEART RATE:\n"
        f"  Avg: {analysis_data['avg_hr']} | Max: {analysis_data['max_hr']}\n\n"
        f"COACHING TASK:\n"
        f"  Session type is '{activity_type}' (training run vs race — adjust expectations accordingly).\n"
        f"  1. Use EXACT values from tables. Do not round or guess.\n"
        f"  2. If any metric shows MISSING, state it upfront. Do not assume.\n"
        f"  3. Provide: Session Summary | Key Insights | HR Analysis | Room for Improvement | Next Step."
    )
    return prompt


# ── Coaching Pipeline (root_agent) ────────────────────────────────────────────
class CoachingPipeline(BaseAgent):
    """
    Orchestrates the 3-agent pipeline for ADK.
    ADK calls _run_async_impl() for every user turn:
      Observe (DataFetcherAgent) → Think (DataAnalystAgent) → Act (CoachAgent)
    Follow-up turns (Repeat): reloads session.state["last_activity"].
    """
    class Config:
        arbitrary_types_allowed = True

    name: str = "athlete_analyzer"
    description: str = "Elite AI running coach. Analyses your Strava activities to provide personalized insights."

    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        """ADK entry point. Unpacks JSON payload from user content."""
        user_text = ""
        if ctx.user_content and ctx.user_content.parts:
            user_text = ctx.user_content.parts[0].text or ""

        try:
            payload = json.loads(user_text)
        except Exception:
            payload = {"message": user_text}

        message       = payload.get("message", "")
        activity_id   = payload.get("activity_id")
        activity_type = payload.get("activity_type", "training")
        analysis_data = payload.get("analysis_data", None)

        user_id    = ctx.session.user_id if ctx.session else "unknown"
        session_id = ctx.session.id      if ctx.session else "unknown"

        logger.info(
            f"[Pipeline] user_id={user_id} | session={session_id} | "
            f"activity_id={activity_id} | activity_type={activity_type} | "
            f"message='{message[:60]}'"
        )

        async for part in self._run_pipeline(
            user_id, session_id, ctx,
            message, activity_id, activity_type, analysis_data
        ):
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(
                    role="model",
                    parts=[types.Part.from_text(text=part)],
                ),
            )

    async def _run_pipeline(
        self,
        user_id: str,
        session_id: str,
        ctx: InvocationContext,
        message: str,
        activity_id: Optional[int],
        activity_type: str,
        analysis_data: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Multi-agent pipeline:
          Stage 2 — LLM coaching analysis (CoachAgent)
          Follow-up — reload session context, route to general coach
        """
        loop = asyncio.get_event_loop()

        # ── A: Activity Analysis Request ──────────────────────────────────────
        if activity_id is not None and analysis_data is not None:
            # Persist in session state for follow-up turns
            if ctx.session and hasattr(ctx.session, "state"):
                ctx.session.state["last_activity"] = analysis_data
                ctx.session.state["last_activity_type"] = activity_type
                logger.info("[Pipeline] Stored analysis_data in session.state['last_activity']")

            # Stage 2 — ACT: Build prompt + LLM call
            analyst_prompt = await loop.run_in_executor(
                None, _build_analyst_prompt, analysis_data, activity_type
            )

            coaching_report = await loop.run_in_executor(
                None,
                lambda: _call_specialist(
                    PROMPTS["activity_analyst"]["instruction"],
                    analyst_prompt,
                    label="CoachAgent/Analyze",
                ),
            )
            yield coaching_report
            return

        # ── B: Follow-up Chat ─────────────────────────────────────────────────
        # REPEAT: load previous activity from session state if available
        last_activity      = None
        last_activity_type = "training"
        if ctx.session and hasattr(ctx.session, "state"):
            last_activity      = ctx.session.state.get("last_activity")
            last_activity_type = ctx.session.state.get("last_activity_type", "training")

        if last_activity:
            logger.info("[Pipeline/FollowUp] Attaching session activity context to general coach prompt")
            summary = (
                f"Activity: {last_activity.get('activity_name')} | "
                f"Distance: {last_activity.get('total_distance_km')} km | "
                f"Time: {last_activity.get('total_time')} | "
                f"Avg Pace: {last_activity.get('avg_pace_overall')}/km | "
                f"Avg HR: {last_activity.get('avg_hr')} | "
                f"Session Type: {last_activity_type}"
            )
            enriched_message = (
                f"PREVIOUS ACTIVITY CONTEXT:\n{summary}\n\n"
                f"ATHLETE'S FOLLOW-UP QUESTION:\n{message}"
            )
        else:
            enriched_message = message

        # THINK: Classify remaining intents if no activity context
        if not message.strip():
            yield "Please ask me a question or share an activity to analyze!"
            return

       
        final_resp = await loop.run_in_executor(
            None,
            lambda: _call_specialist(
                PROMPTS["general_coach"]["instruction"],
                enriched_message,
                label="CoachAgent/Chat",
            ),
        )
        yield final_resp


# ── Coaching Engine — Session Management ──────────────────────────────────────
class CoachingEngine:
    """
    Wraps CoachingPipeline with Vertex AI session management.
    user_id = strava_athlete_id (no hardcoded defaults).
    Vertex AI session ONLY — will raise if not configured.
    """

    def __init__(self):
        agent_engine_id = os.getenv("VERTEX_AGENT_ENGINE_ID")
        if not agent_engine_id:
            raise EnvironmentError(
                "VERTEX_AGENT_ENGINE_ID env var is required. "
                "InMemory session is not supported — configure Vertex AI Agent Engine."
            )

        self.pipeline = CoachingPipeline()
        self.session_service = VertexAiSessionService(
            project=_project_id,
            location=_location,
            agent_engine_id=agent_engine_id,
        )
        logger.info(f"[CoachingEngine] Vertex AI Session Service ready (engine={agent_engine_id})")

        self.runner = Runner(
            app_name="athlete-analyzer",
            agent=self.pipeline,
            session_service=self.session_service,
        )
        logger.info(f"[CoachingEngine] Ready | model={_model_name}")

    async def _get_or_create_session(self, user_id: str, session_id: Optional[str]) -> str:
        """Resume existing session or create a new one keyed on strava_athlete_id."""
        if session_id:
            try:
                session = await self.session_service.get_session(
                    app_name=self.runner.app_name,
                    user_id=user_id,
                    session_id=session_id,
                )
                if session:
                    logger.info(f"[Session] Resumed session={session_id} for user={user_id}")
                    return session_id
            except Exception as e:
                logger.warning(f"Failed to fetch session {session_id}, falling back safely: {e}")
        
        try:
            new_session = await self.session_service.create_session(
                app_name=self.runner.app_name,
                user_id=user_id,
            )
            logger.info(f"[Session] Created session={new_session.id} for user={user_id}")
            return new_session.id
        except Exception as e:
            logger.error(f"Failed to create remote session: {e}")
            return f"fallback_{int(time.time())}"

    async def chat_async(
        self,
        message: str,
        user_id: str,           # ← strava_athlete_id, required
        session_id: Optional[str] = None,
        activity_id: Optional[int] = None,
        activity_type: str = "training",
    ) -> AsyncGenerator[Dict[str, str], None]:
        """
        Main entry. Streams {text, session_id} dicts to the API layer.
        user_id = strava_athlete_id (enforced — no default).
        """
        if not user_id:
            raise ValueError("user_id (strava_athlete_id) is required.")

        t_start = time.time()
        session_id = await self._get_or_create_session(user_id, session_id)
        
        loop = asyncio.get_event_loop()
        analysis_data = None
        athlete_name = "Athlete"
        
        # ── Step 0 & 1: Do slow external IO *before* touching ADK Runner ──
        if activity_id is not None:
            yield {"text": " Looking up your athlete profile...", "session_id": session_id}
            
            from services.db_service import get_db_service
            db = get_db_service()

            logger.info(f"[Stage0/DataFetcher] BigQuery lookup for strava_id={user_id}")
            profile = db.get_athlete_profile_by_strava_id(user_id)

            if not profile:
                yield {"text": f"\n\n Athlete with Strava ID `{user_id}` not found in the database. Please check the ID.", "session_id": session_id}
                return

            refresh_token = profile.get("refresh_token")
            athlete_name  = profile.get("firstname") or profile.get("name") or "Athlete"

            if not refresh_token:
                yield {"text": f"\n\n No Strava refresh token found for athlete `{athlete_name}`. Please reconnect your Strava account.", "session_id": session_id}
                return

            logger.info(f"[Stage0/DataFetcher] Found athlete: {athlete_name}")

            yield {"text": f"\n\n📡 Connecting to Strava to fetch activity {activity_id}...", "session_id": session_id}
            
            from tools.coach_tools import analyze_activity_deep
            analysis_data = await loop.run_in_executor(
                None,
                analyze_activity_deep,
                activity_id,
                refresh_token,
                athlete_name,
            )

            if "error" in analysis_data:
                yield {"text": f"\n\n {analysis_data['error']}", "session_id": session_id}
                return

            yield {"text": f"\n\n Coaching review in progress for {analysis_data['athlete_name']}...", "session_id": session_id}

        # ── Step 2: Now we pass the assembled payload to the Runner ──
        # Runner will automatically sync this to Vertex AI Session Service (Batching!)
        payload = {
            "message":       message,
            "activity_id":   activity_id,
            "activity_type": activity_type,
            "analysis_data": analysis_data,
        }
        user_message = types.Content(
            role="user",
            parts=[types.Part(text=json.dumps(payload))],
        )

        async for event in self.runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=user_message,
        ):
            if event.content:
                if isinstance(event.content, str):
                    yield {"text": event.content, "session_id": session_id}
                elif hasattr(event.content, "parts") and event.content.parts:
                    text = event.content.parts[0].text
                    if text:
                        yield {"text": text, "session_id": session_id}

        t_end = time.time()
        total_time = t_end - t_start
        yield {"text": f"\n\n⏱️ Total processing time: {total_time:.2f} seconds.", "session_id": session_id}

    @property
    def agent(self):
        return self.pipeline


# ── Singleton ─────────────────────────────────────────────────────────────────
coaching_engine = CoachingEngine()
