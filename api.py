"""
FastAPI — Athlete Analyzer API
================================
Endpoints:
  POST /api/analyze  — analyze a Strava activity (primary entry point)
  POST /api/chat     — follow-up coaching chat
  GET  /health       — health check
"""

import os
import logging
from typing import Optional
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
        "1. `POST /api/analyze` — pass your Strava athlete ID + activity ID + activity type.\n"
        "2. The system fetches your profile from BigQuery, pulls Strava streams, and streams a coaching report.\n"
        "3. Copy the `session_id` from the response.\n"
        "4. `POST /api/chat` — ask follow-up questions using the same session.\n\n"
        "**No tokens or credentials are passed from the frontend.**"
    ),
    version="2.0.0",
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
            async for chunk in engine.chat_async(
                message=request.message,
                user_id=request.user_id,
                session_id=request.session_id,
            ):
                yield json.dumps(chunk) + "\n"
        except Exception as e:
            logger.error(f"[API/chat] Error: {e}", exc_info=True)
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")
