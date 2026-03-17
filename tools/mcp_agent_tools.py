from typing import Dict, Any, Optional
from utils.mcp_client import mcp_client
import logging

logger = logging.getLogger(__name__)

async def get_my_dashboard_summary(strava_id: str, time_frame: str = "monthly") -> Dict[str, Any]:
    """
    Fetches the athlete's dashboard summary including distance, points, and elevation.
    Use this when the user asks for their progress, total distance, or current stats.
    """
    return await mcp_client.call_tool(
        strava_id, 
        "get_my_dashboard_summary", 
        {"time_frame": time_frame}
    )

async def get_my_race_history(strava_id: str) -> Dict[str, Any]:
    """
    Fetches all historical race results for the athlete.
    Use this when the user asks about past races, PRs (Personal Records), or race frequency.
    """
    return await mcp_client.call_tool(
        strava_id, 
        "get_my_race_history", 
        {}
    )

async def get_my_2026_goals(strava_id: str) -> Dict[str, Any]:
    """
    Retrieves the athlete's primary goals for the year 2026.
    Use this to give context-aware advice or check if the athlete is on track for their goals.
    """
    # Note: Backend tool is named 'get_my_2026_goal' (singular)
    return await mcp_client.call_tool(
        strava_id, 
        "get_my_2026_goal", 
        {}
    )

# Map for the tool executor
MCP_TOOLS = {
    "get_my_dashboard_summary": get_my_dashboard_summary,
    "get_my_race_history": get_my_race_history,
    "get_my_2026_goals": get_my_2026_goals
}
