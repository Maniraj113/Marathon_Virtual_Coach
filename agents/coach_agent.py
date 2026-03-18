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
from google.adk.memory.vertex_ai_memory_bank_service import VertexAiMemoryBankService
from google.genai import types
from pydantic import BaseModel
from vertexai.generative_models import GenerativeModel

import vertexai
from google import genai
from utils.agent_helpers import build_analyst_prompt, extract_json

load_dotenv()
logger = logging.getLogger(__name__)

# ── Vertex AI Initialization ──────────────────────────────────────────────────
_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
_location   = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
_model_name = os.getenv("COACH_AGENT_MODEL", "gemini-2.5-flash-lite")

if not _project_id:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT env var is required.")

vertexai.init(project=_project_id, location=_location)

# ── Prompts ───────────────────────────────────────────────────────────────────
_PROMPTS_PATH = Path(__file__).parent.parent / "config" / "prompts.yaml"
with open(_PROMPTS_PATH, "r") as f:
    PROMPTS = yaml.safe_load(f)


# ── Pipeline State ────────────────────────────────────────────────────────────
# ── Shared GenAI Client (Reused for Performance) ──────────────────────────────
_genai_client = genai.Client(vertexai=True, project=_project_id, location=_location)

async def _stream_specialist(system_instruction: str, user_message: Any, label: str = "LLM") -> AsyncGenerator[str, None]:
    """
    CoachAgent — Streams Gemini response via Vertex AI.
    Yields chunks as they arrive.
    """
    logger.info(f"[Streaming:{label}] Starting stream for model={_model_name}")
    
    # Use the shared client's .aio interface
    try:
        t0 = time.perf_counter()
        stream = await _genai_client.aio.models.generate_content_stream(
            model=_model_name,
            contents=user_message,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction
            )
        )
        last_chunk = None
        async for chunk in stream:
            last_chunk = chunk
            if chunk.text:
                yield {"text": chunk.text}
        
        latency_ms = int((time.perf_counter() - t0) * 1000)
        
        # Log telemetry after stream completes
        try:
            usage = getattr(last_chunk, "usage_metadata", None)
            if usage:
                logger.info(
                    f"[Telemetry:{label}/Stream] model={_model_name} "
                    f"| latency={latency_ms}ms "
                    f"| prompt_tokens={usage.prompt_token_count} "
                    f"| candidate_tokens={usage.candidates_token_count}"
                )
                metrics = {
                    "model": _model_name,
                    "prompt_tokens": usage.prompt_token_count,
                    "candidate_tokens": usage.candidates_token_count,
                    "total_tokens": usage.total_token_count,
                    "stream_latency_ms": latency_ms
                }
                yield {"text": f"\n\n<!--TELEMETRY:{json.dumps(metrics)}-->"}
        except Exception as e:
            logger.error(f"[Telemetry] Exception getting usage: {e}")
            
    except Exception as e:
        logger.error(f"[Streaming:{label}] Error during stream: {e}", exc_info=True)
        yield {"text": "\n\n [AI Agent Error: Stream interrupted] "}


def _call_specialist(system_instruction: str, user_message: str, label: str = "LLM") -> str:
    """
    CoachAgent — calls Gemini via Vertex AI (Sync/One-shot).
    Used when we don't need streaming (unlikely in this chat).
    """
    t0 = time.perf_counter()
    response = _genai_client.models.generate_content(
        model=_model_name,
        contents=user_message,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction
        )
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)
    
    usage = getattr(response, "usage_metadata", None)
    if usage:
        logger.info(f"[Telemetry:{label}] model={_model_name} | latency={latency_ms}ms | tokens={usage.total_token_count}")

    return response.text


class AthleteState(BaseModel):
    """Structured state passed between pipeline stages."""
    user_id:        str = ""
    session_id:     str = ""
    original_query: str = ""
    intent:         str = ""
    activity_type:  str = ""
    final_response: str = ""


# ── LLM Specialist Caller with Telemetry ─────────────────────────────────────
# The original _call_specialist function was here. It has been moved and modified above.


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

    memory_service: Any = None

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
        memory_context= payload.get("memory_context", "No previous memory facts available.")
        history_rows  = payload.get("history_rows", [])

        user_id    = ctx.session.user_id if ctx.session else "unknown"
        session_id = ctx.session.id      if ctx.session else "unknown"

        logger.info(
            f"[Pipeline] user_id={user_id} | session={session_id} | "
            f"activity_id={activity_id} | activity_type={activity_type} | "
            f"message='{message[:60]}'"
        )

        async for part in self._run_pipeline(
            user_id, session_id, ctx,
            message, activity_id, activity_type, analysis_data, memory_context, history_rows
        ):
            text_val = part.get("text", "") if isinstance(part, dict) else str(part)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(
                    role="model",
                    parts=[types.Part.from_text(text=text_val)],
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
        memory_context: str = "No previous memory facts available.",
        history_rows: Optional[list] = None,
    ) -> AsyncGenerator[str, None]:
        if history_rows is None:
            history_rows = []
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
                None, build_analyst_prompt, analysis_data, activity_type, memory_context
            )

            # Stream the coaching report directly for better UX
            async for chunk in _stream_specialist(
                PROMPTS["activity_analyst"]["instruction"],
                analyst_prompt,
                label="CoachAgent/Analyze",
            ):
                yield chunk
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
                f"ATHLETE LONG-TERM CONTEXT (Memory Bank):\n{memory_context}\n\n"
                f"ATHLETE'S FOLLOW-UP QUESTION:\n{message}"
            )
        else:
            enriched_message = (
                f"ATHLETE LONG-TERM CONTEXT (Memory Bank):\n{memory_context}\n\n"
                f"ATHLETE'S QUESTION:\n{message}"
            )

        # THINK: Classify remaining intents if no activity context
        if not message.strip():
            yield {"text": "Please ask me a question or share an activity to analyze!", "session_id": session_id}
            return

        # ── INTERCEPT: Use MCP Tools for general questions ──
        if not last_activity:
            classification_prompt = f"""
            Given the following user query, which external fitness data do they need?
            Options:
            - SUMMARY: if they want total stats, distance, progress, or dashboard stats.
            - RACE_HISTORY: if they ask about past races, PRs, or race history.
            - GOALS: if they ask for their goals, 2026 goals, or targets.
            - NONE: if none of the above.
            
            Query: {message}

            Return ONLY the option name (SUMMARY, RACE_HISTORY, GOALS, NONE) as plain text.
            """
            try:
                # Use a fast call to determine intent
                tool_needed = await loop.run_in_executor(
                    None, _call_specialist, 
                    "You are an intent router. Return ONLY the requested Option string exactly.", 
                    classification_prompt, "ToolRouter"
                )
                tool_needed = tool_needed.strip().upper()
                
                from tools.mcp_agent_tools import get_my_dashboard_summary, get_my_race_history, get_my_2026_goals
                mcp_data = None
                
                if "SUMMARY" in tool_needed:
                    yield {"text": "\n\n *Fetching your dashboard stats...*\n\n", "session_id": session_id}
                    mcp_data = await get_my_dashboard_summary(user_id, "yearly")
                    enriched_message += f"\n\nCURRENT STATS (From MCP Server):\n{json.dumps(mcp_data)}"
                elif "RACE_HISTORY" in tool_needed:
                    yield {"text": "\n\n *Fetching your race history...*\n\n", "session_id": session_id}
                    mcp_data = await get_my_race_history(user_id)
                    enriched_message += f"\n\nRACE HISTORY (From MCP Server):\n{json.dumps(mcp_data)}"
                elif "GOALS" in tool_needed:
                    yield {"text": "\n\n *Checking your goals...*\n\n", "session_id": session_id}
                    mcp_data = await get_my_2026_goals(user_id)
                    enriched_message += f"\n\nGOALS (From MCP Server):\n{json.dumps(mcp_data)}"
            except Exception as e:
                logger.error(f"Failed to fetch MCP data via routing: {e}")



       
        # Build chat history from pre-fetched rows
        try:
            contents_list = []
            for row in history_rows:
                role_val = row.get("role", "user")
                if role_val not in ["user", "model"]:
                    role_val = "user"
                    
                contents_list.append(types.Content(
                    role=role_val, 
                    parts=[types.Part.from_text(text=str(row.get("content", "")))]
                ))
            
            # Add the *current* enriched message 
            contents_list.append(types.Content(
                role="user", 
                parts=[types.Part.from_text(text=enriched_message)]
            ))
        except Exception as e:
            logger.error(f"Failed to assemble chat history: {e}")
            # Fallback
            contents_list = [types.Content(role="user", parts=[types.Part.from_text(text=enriched_message)])]

        # Route to general coach
        async for chunk in _stream_specialist(
            PROMPTS["general_coach"]["instruction"],
            contents_list,
            label="CoachAgent/Chat",
        ):
            chunk["session_id"] = session_id
            yield chunk


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

        self.session_service = VertexAiSessionService(
            project=_project_id,
            location=_location,
            agent_engine_id=agent_engine_id,
        )
        self.memory_service = VertexAiMemoryBankService(
            project=_project_id,
            location=_location,
            agent_engine_id=agent_engine_id,
        )
        self.pipeline = CoachingPipeline(memory_service=self.memory_service)
        logger.info(f"[CoachingEngine] Vertex AI Session Service ready (engine={agent_engine_id})")

        self.runner = Runner(
            app_name="athlete-analyzer",
            agent=self.pipeline,
            session_service=self.session_service,
            memory_service=self.memory_service,
        )
        logger.info(f"[CoachingEngine] Ready | model={_model_name}")

    async def _get_or_create_session(self, user_id: str, session_id: Optional[str]) -> str:
        """Resume existing session or create a new one keyed on strava_athlete_id."""
        # Vertex AI session IDs are long numeric strings.
        # If the frontend passes a generated ID like 'general_...' or 'local_...',
        # do NOT try to fetch it from Vertex AI (which causes long aiohttp timeouts).
        is_valid_remote_id = session_id and session_id.isdigit()

        if is_valid_remote_id:
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
        t_init = time.time()
        session_id = await self._get_or_create_session(user_id, session_id)
        logger.info(f"[Perf] Session resolution took: {time.time() - t_init:.2f}s")
        
        loop = asyncio.get_event_loop()
        analysis_data = None
        athlete_name = "Athlete"
        
        from services.db_service import get_db_service
        db = get_db_service()

        # ── Step 0 & 1: Parallelize Independent IO ──
        yield {"text": " 🧠 Gathering athlete context and history...", "session_id": session_id}
        t_io = time.time()

        async def fetch_memory():
            if self.memory_service:
                try:
                    docs = await self.memory_service.search_memory(
                        app_name="athlete-analyzer",
                        user_id=user_id,
                        query="athlete personal details, weight, age, injuries, goals, and training history"
                    )
                    if docs:
                        return "\\n".join(getattr(d, "text", str(d)) for d in docs)
                except Exception as e:
                    logger.warning(f"Memory search failed: {e}")
            return "No previous memory facts available."

        async def get_profile_async():
            if activity_id is not None:
                return await loop.run_in_executor(None, db.get_athlete_profile_by_strava_id, user_id)
            return None

        async def get_history_async():
            return await loop.run_in_executor(None, db.list_chat_messages, session_id)

        profile, memory_context, raw_history = await asyncio.gather(
            get_profile_async(),
            fetch_memory(),
            get_history_async()
        )
        logger.info(f"[Perf] Parallel IO took: {time.time() - t_io:.2f}s")

        # Convert history so it's JSON serializable
        json_safe_history = []
        if raw_history:
            for row in raw_history[-10:]:
                json_safe_history.append({
                    "role": row.get("role", "user"),
                    "content": str(row.get("content", ""))
                })

        if activity_id is not None:
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
            
            t_strava = time.time()
            from tools.coach_tools import analyze_activity_deep
            analysis_data = await analyze_activity_deep(
                activity_id,
                refresh_token,
                athlete_name,
            )
            logger.info(f"[Perf] Strava fetch took: {time.time() - t_strava:.2f}s")

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
            "memory_context": memory_context,
            "history_rows":  json_safe_history
        }
        user_message = types.Content(
            role="user",
            parts=[types.Part(text=json.dumps(payload))],
        )

        full_agent_response = ""
        telemetry_metrics = None
        async for event in self.runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=user_message,
        ):
            if event.content:
                text_part = ""
                if isinstance(event.content, str):
                    text_part = event.content
                elif hasattr(event.content, "parts") and event.content.parts:
                    text_part = event.content.parts[0].text
                
                if text_part:
                    if "<!--TELEMETRY:" in text_part:
                        import re
                        m = re.search(r'<!--TELEMETRY:(.*?)-->', text_part)
                        if m:
                            try:
                                telemetry_metrics = json.loads(m.group(1))
                            except Exception as e:
                                logger.warning(f"Failed to parse telemetry: {e}")
                        text_part = re.sub(r'<!--TELEMETRY:.*?-->', '', text_part)
                    
                    if text_part:
                        full_agent_response += text_part
                        yield {"text": text_part, "session_id": session_id}

        # Calculate processing time and show to user
        total_time_s = time.time() - t_start
        processing_time_str = f"\n\n⏱️ **Processing Time:** {total_time_s:.2f}s"
        full_agent_response += processing_time_str
        yield {"text": processing_time_str, "session_id": session_id}

        # Save Chat History to BigQuery
        try:
            from services.db_service import get_db_service
            db = get_db_service()
            if message.strip():
                db.save_chat_message(user_id, session_id, "user", message)
            if full_agent_response:
                db.save_chat_message(user_id, session_id, "model", full_agent_response)
                
            # Log Telemetry to DB!
            if telemetry_metrics and hasattr(db, "save_telemetry"):
                db.save_telemetry(
                    strava_athlete_id=user_id,
                    session_id=session_id,
                    model=telemetry_metrics.get("model", _model_name),
                    prompt_tokens=telemetry_metrics.get("prompt_tokens", 0),
                    candidate_tokens=telemetry_metrics.get("candidate_tokens", 0),
                    total_tokens=telemetry_metrics.get("total_tokens", 0),
                    turn_latency_ms=telemetry_metrics.get("stream_latency_ms", 0),
                    total_processing_ms=int(total_time_s * 1000)
                )
        except Exception as e:
            logger.error(f"Failed to save history/telemetry: {e}")

        # Intelligent Memory Extraction (Only save important and permanent facts)
        if self.memory_service and message.strip():
            try:
                # We use asyncio to run this in the background without blocking the user response
                async def extract_memory():
                    try:
                        # Re-fetch the session block
                        session = await self.session_service.get_session(
                            app_name=self.runner.app_name,
                            user_id=user_id,
                            session_id=session_id
                        )
                        if session:
                            await self.memory_service.add_session_to_memory(session)
                            logger.info(f"Memory extraction completed for user {user_id}")
                    except Exception as inner_e:
                        logger.error(f"Failed inner memory extraction: {inner_e}")
                
                # Fire and forget memory extraction
                loop.create_task(extract_memory())
            except Exception as e:
                logger.error(f"Failed to schedule memory extraction: {e}")

    @property
    def agent(self):
        return self.pipeline


# ── Singleton ─────────────────────────────────────────────────────────────────
coaching_engine = CoachingEngine()

