import os
import asyncio
import logging
import httpx
from typing import Optional, Dict, Any
from mcp import ClientSession
from mcp.client.sse import sse_client

logger = logging.getLogger(__name__)

# Configurable backend URL
BACKEND_URL = os.getenv("FITNESS_BACKEND_URL", "http://localhost:8000")

class MCPClient:
    """
    Handles robust communication with the Fitness Backend MCP server via SSE.
    """
    _token_cache: Dict[str, str] = {}

    async def _get_athlete_token(self, strava_id: str) -> Optional[str]:
        """Fetch the MCP token via the Backend REST API (Internal Service Call)."""
        if strava_id in self._token_cache:
            return self._token_cache[strava_id]
        
        try:
            # Internal call to get or generate the token
            async with httpx.AsyncClient() as client:
                url = f"{BACKEND_URL}/api/athletes/mcp-token/{strava_id}"
                resp = await client.post(url)
                if resp.status_code == 200:
                    token = resp.json().get("token")
                    if token:
                        self._token_cache[strava_id] = token
                        return token
                else:
                    logger.error(f"Failed to fetch token from backend: {resp.status_code}")
        except Exception as e:
            logger.error(f"Error calling token endpoint for {strava_id}: {e}")
        return None

    async def call_tool(self, strava_id: str, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """
        Connects to the Backend MCP server via SSE and invokes a tool.
        Automatically injects the athlete's PAT token.
        """
        token = await self._get_athlete_token(strava_id)
        if not token:
            return {"error": f"No MCP access token found for athlete {strava_id}."}

        # Inject token into arguments
        arguments["token"] = token

        try:
            sse_url = f"{BACKEND_URL}/mcp/sse"
            
            async with sse_client(sse_url) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)
                    
                    if hasattr(result, 'content') and result.content:
                        # MCP often returns JSON inside the text content
                        text_payload = result.content[0].text
                        try:
                            import json
                            return json.loads(text_payload)
                        except (json.JSONDecodeError, TypeError):
                            return text_payload
                    return result
                    
        except Exception as e:
            logger.error(f"MCP Tool Call Error ({tool_name}): {e}")
            return {"error": f"Failed to communicate with fitness tools: {str(e)}"}

mcp_client = MCPClient()
