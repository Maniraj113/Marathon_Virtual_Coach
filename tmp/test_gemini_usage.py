import asyncio
import os
import vertexai
from google import genai

async def test_stream():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    vertexai.init(project=project_id, location=location)
    client = genai.Client(vertexai=True, project=project_id, location=location)
    
    stream = await client.aio.models.generate_content_stream(
        model="gemini-2.5-flash-lite",
        contents="Hi, say 10 words.",
    )
    res = []
    async for chunk in stream:
        res.append(f"chunk metadata: {getattr(chunk, 'usage_metadata', 'No metadata')}")
    
    res.append(f"stream object metadata: {getattr(stream, 'usage_metadata', 'No stream metadata')}")
    with open('tmp/usage.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(res))

asyncio.run(test_stream())
