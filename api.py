"""
FastAPI — Athlete Analyzer API
================================
Endpoints:
  POST /api/analyze        — analyze a Strava activity (primary entry point)
  POST /api/chat           — follow-up coaching chat (uses existing session)
  GET  /api/sessions/{..}  — retrieve cached analysis session for an activity
  POST /api/sessions       — save/update analysis session with cached result
  GET  /api/history/{..}   — list recent analysis history for an athlete
  GET  /health             — health check
"""

import os
import logging
from typing import Optional, List
from services.bq_sync import BigQuerySync
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import json

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ── Request / Response Models ─────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    """Primary entry point — analyze a specific Strava activity."""
    strava_athlete_id: str = Field(
        ...,
        description="Strava athlete ID (used as user_id for session management).",
        json_schema_extra={"example": "12345678"},
    )
    strava_activity_id: int = Field(
        ...,
        description="Strava activity ID to analyze.",
        json_schema_extra={"example": 16929808992},
    )
    activity_type: str = Field(
        default="training",
        description="Session type: 'training' or 'race'. Affects coaching tone.",
        json_schema_extra={"example": "training"},
    )
    message: Optional[str] = Field(
        default=None,
        description="Optional initial question or context from the athlete.",
    )
    session_id: Optional[str] = Field(
        default=None,
        description="Existing session ID to continue. Leave empty for a new session.",
    )


class ChatRequest(BaseModel):
    """Follow-up coaching chat — continues a previous analyze session."""
    message: str = Field(
        ...,
        description="Athlete's follow-up question.",
        json_schema_extra={"example": "Why was my pace slow in the last km?"},
    )
    user_id: str = Field(
        ...,
        description="Strava athlete ID (must match the analyze session).",
        json_schema_extra={"example": "12345678"},
    )
    session_id: str = Field(
        ...,
        description="Session ID returned from /api/analyze.",
    )


class SaveSessionRequest(BaseModel):
    """Request to persist an analysis session to BigQuery."""
    strava_athlete_id: str
    strava_activity_id: int
    session_id: str
    cached_analysis: str
    activity_type: str = "training"


class HealthResponse(BaseModel):
    status: str
    agent_name: str
    project_id: Optional[str]


# ── App Lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    from coach_agent import coaching_engine
    app.state.engine = coaching_engine
    logger.info("🏃 Coaching Engine initialized.")
    yield
    logger.info("🛑 Shutdown.")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="🏃 Athlete Analyzer — Coach Miles API",
    description=(
        "AI running coach powered by Strava + BigQuery + Gemini.\n\n"
        "**Flow:**\n"
        "1. `GET /api/sessions/{athlete_id}/{activity_id}` — check for cached analysis first.\n"
        "2. `POST /api/analyze` — only if no cached session; pass strava_athlete_id + activity_id + activity_type.\n"
        "3. `POST /api/sessions` — save the session_id + analysis after step 2.\n"
        "4. `POST /api/chat` — follow-up questions using the stored session_id.\n"
        "5. `GET /api/history/{athlete_id}` — get history of all analyzed activities.\n\n"
        "**No tokens or credentials are passed from the frontend.**"
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Must be False when allow_origins is ["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs")


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    engine = app.state.engine
    return HealthResponse(
        status="healthy",
        agent_name=getattr(engine.agent, "name", "unknown"),
        project_id=os.getenv("GOOGLE_CLOUD_PROJECT"),
    )


@app.get(
    "/api/sessions/{strava_athlete_id}/{strava_activity_id}",
    tags=["Sessions"],
    summary="Get cached analysis session for an activity",
)
async def get_analysis_session(strava_athlete_id: str, strava_activity_id: int):
    """
    Check if a cached analysis session already exists for this athlete + activity.
    Returns `{session_id, cached_analysis, activity_type, created_at}` or `{found: false}`.
    """
    from services.db_service import get_db_service
    db = get_db_service()
    session = db.get_analysis_session(strava_athlete_id, strava_activity_id)
    if session:
        logger.info(
            f"[API/sessions] Cache HIT — athlete={strava_athlete_id} activity={strava_activity_id}"
        )
        return {"found": True, **session}
    logger.info(
        f"[API/sessions] Cache MISS — athlete={strava_athlete_id} activity={strava_activity_id}"
    )
    return {"found": False}


@app.post(
    "/api/sessions",
    tags=["Sessions"],
    summary="Save analysis session to BigQuery",
)
async def save_analysis_session(request: SaveSessionRequest):
    """
    Persist the session_id and cached analysis result for this athlete + activity.
    Call this after a successful /api/analyze to avoid re-analysis on next open.
    """
    from services.db_service import get_db_service
    db = get_db_service()
    ok = db.save_analysis_session(
        strava_athlete_id=request.strava_athlete_id,
        strava_activity_id=request.strava_activity_id,
        session_id=request.session_id,
        cached_analysis=request.cached_analysis,
        activity_type=request.activity_type,
    )
    if ok:
        return {"status": "saved"}
    raise HTTPException(status_code=500, detail="Failed to save session to BigQuery.")


@app.get(
    "/api/history/{strava_athlete_id}",
    tags=["Sessions"],
    summary="List recent analysis history for an athlete",
)
async def get_analysis_history(strava_athlete_id: str, limit: int = 20):
    """
    Returns a list of recent analyses for the sidebar history panel.
    Each entry: {session_id, strava_activity_id, activity_type, created_at}.
    """
    from services.db_service import get_db_service
    db = get_db_service()
    history = db.list_analysis_sessions(strava_athlete_id, limit=limit)
    return {"history": history}


@app.post(
    "/api/analyze",
    tags=["Coaching"],
    summary="Analyze a Strava activity",
    response_description="Streaming ndjson coaching report",
)
async def analyze_activity(request: AnalyzeRequest):
    """
    Primary endpoint. Pass strava_athlete_id + strava_activity_id + activity_type.

    The system will:
    1. Look up the athlete in BigQuery using strava_athlete_id
    2. Fetch Strava activity streams (pace, HR) using the stored refresh token
    3. Clean and structure the data
    4. Stream a personalized coaching report

    Returns a stream of ndjson objects:
      `{"text": "...", "session_id": "..."}` — coaching content chunks
    """
    engine = app.state.engine

    logger.info(
        f"[API/analyze] strava_athlete_id={request.strava_athlete_id} | "
        f"activity_id={request.strava_activity_id} | "
        f"activity_type={request.activity_type}"
    )

    async def event_generator():
        try:
            async for chunk in engine.chat_async(
                message=request.message or "",
                user_id=str(request.strava_athlete_id),
                session_id=request.session_id,
                activity_id=request.strava_activity_id,
                activity_type=request.activity_type,
            ):
                yield json.dumps(chunk) + "\n"
        except Exception as e:
            logger.error(f"[API/analyze] Error: {e}", exc_info=True)
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")


@app.post(
    "/api/chat",
    tags=["Coaching"],
    summary="Follow-up coaching chat",
    response_description="Streaming ndjson coaching response",
)
async def chat(request: ChatRequest):
    """
    Continue coaching conversation from a previous /api/analyze session.
    Pass the session_id returned from /api/analyze to maintain context.
    The agent loads the previous activity from session state — no re-analysis.

    Returns a stream of ndjson objects:
      `{"text": "...", "session_id": "..."}` — coaching response chunks
    """
    engine = app.state.engine

    logger.info(
        f"[API/chat] user_id={request.user_id} | "
        f"session_id={request.session_id} | "
        f"message='{request.message[:60]}'"
    )

    async def event_generator():
        try:
            # Note: activity_id=None signals a follow-up chat, not a new analysis
            async for chunk in engine.chat_async(
                message=request.message,
                user_id=request.user_id,
                session_id=request.session_id,
                activity_id=None,   # ← crucial: tells pipeline NOT to re-fetch Strava data
            ):
                yield json.dumps(chunk) + "\n"
        except Exception as e:
            logger.error(f"[API/chat] Error: {e}", exc_info=True)
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")


@app.post(
    "/api/sync",
    tags=["Database Sync"],
    summary="Trigger BigQuery sync from PostgreSQL",
    response_description="Sync status",
)
async def sync_data():
    """
    Trigger a full sync of all athlete-related tables from the Supabase PostgreSQL
    database to Google BigQuery.
    
    This endpoint is intended to be called by Cloud Scheduler on a daily basis.
    """
    try:
        tables = [
            "athletes", 
            "activities", 
            "race_results", 
            "personal_details", 
            "coach_session_logs", 
            "meal_logs", 
            "wellness_logs",
            "challenge_activities", 
            "challenge_participants"
        ]
        
        sync_manager = BigQuerySync()
        sync_manager.run_full_sync(tables)
        
        return {"status": "success", "message": "All tables synced successfully to BigQuery."}
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")
