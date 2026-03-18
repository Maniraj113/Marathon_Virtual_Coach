import asyncio
import os
import sys
sys.path.append('.')
from agents.coach_agent import coaching_engine
from dotenv import load_dotenv

load_dotenv()

async def test_coach():
    try:
        async for chunk in coaching_engine.chat_async(
            message="Hi coach, what should I eat after a marathon?",
            user_id="test"
        ):
            print(chunk)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(test_coach())
