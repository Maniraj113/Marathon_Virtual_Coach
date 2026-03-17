# Coach Agent Flow & Concepts Explanation

This document breaks down the functionality of the `coach_agent.py` file, clarifies the concepts you found confusing, and corrects a few minor misunderstandings in your flow description.

---

## 1. Architectural Concepts & Asynchronous Programming

### `AsyncGenerator`
This is a standard Python type hint indicating that a function yields multiple values over time asynchronously. Instead of returning one giant response at the end, the function uses `yield` and `async for` to stream small chunks of text as soon as the LLM generates them.

### `asyncio.get_event_loop()` & `loop.run_in_executor()`
In Python, asynchronous code runs on an "event loop". If you run a normal, blocking synchronous function (like querying BigQuery or calling a non-async external API) on the event loop, it will "freeze" the whole application and other users' streams will get stuck. 
- `asyncio.get_event_loop()` grabs the active loop.
- `loop.run_in_executor(None, function, args...)` tells Python: *"Run this blocking function in a background worker thread so the main async loop can keep streaming."* 
*(Note: `asyncio.to_thread()` is just a newer, shorter Python shortcut that does the exact same thing under the hood).*

### `_stream_specialist` vs `_call_specialist`
- `_stream_specialist`: Connects to Vertex AI and yields chunks of the LLM's response one by one (ideal for giving the user a real-time typing effect).
- `_call_specialist`: Connects to Vertex AI but waits for the *entire* response to finish generating before returning it as a single string (used under the hood for quick intent routing where caching/streaming isn't needed).

---

## 2. ADK Framework Details

### `_run_async_impl` & `yield Event(...)`
Google's ADK (Agent Development Kit) requires every Agent class to have a `_run_async_impl` method. 
Our custom logic happens in `_run_pipeline`, which just yields raw strings. However, ADK expects specific framework objects called `Events`. So inside `_run_async_impl`, we loop over the strings from `_run_pipeline` and wrap them:
```python
yield Event(
    author=self.name,
    content=types.Content(role="model", parts=[...])
)
```
This simply translates our raw text into a formal format that the ADK `Runner` understands.

### `async for event in self.runner.run_async(...)`
The `Runner` is the ADK engine that manages session state and safety limits. When we call `self.runner.run_async(...)` in `chat_async`, it triggers `_run_async_impl` internally. We then use an `async for` loop to catch the `Event` objects coming out of the pipeline and send them back to the frontend API.

### Payload vs. Invocation Context (`ctx`)
*Your Question: "Am I getting confused with invocation context (ctx) and payload? Looks like both are similar and we are mixing both?"*

**The Difference:**
- **Context (`ctx`)**: is the massive object provided by the ADK framework. It includes session data, memory, user IDs, and standard ADK metadata. Standard ADK agents only accept a single string message (e.g., `"How was my run?"`).
- **Payload**: Because we need to pass a lot of custom backend data (like `activity_id`, `activity_type`, and giant `analysis_data`), we use a trick. In `chat_async` (Step 2), we package all of this data into a JSON string and pass it as the standard ADK message. Inside `_run_async_impl`, we unpack that JSON string back into a `payload` dictionary so we can extract our custom variables.

In short: `payload` is our custom data envelope hidden *inside* the standard ADK `ctx` message.

---

## 3. Step-by-Step Flow (Corrections & Clarifications)

Your description was very close! Here is the exact adjusted flow based on the code:

### Application Startup
1. **Coach Engine Initialization:** The `CoachingEngine` class is initialized. It sets up the Vertex AI session service, the memory bank service, the `CoachingPipeline` agent, and the `Runner`. 
   > *Correction:* Sessions are tied to the **`user_id` (Strava Athlete ID)** and a random/existing **`session_id`**. They are **NOT** tied to the combination of userid + activity id. An activity ID is just context stored *inside* an existing session.

### The Request Lifecycle (Calling `chat_async`)

1. **Session Handling:** `_get_or_create_session` fires. If the frontend provides a valid session ID, it reconnects it; otherwise, it creates a new Vertex AI session.
2. **External IO (Step 0 & 1):** Before the ADK pipeline is even touched, `chat_async` checks if an `activity_id` was passed.
   - If yes: It queries BigQuery for the user's Strava tokens, and then calls `analyze_activity_deep` to download the raw Strava data and clean it.
   - *Why do this here?* Because fetching external data can be slow. By doing it outside the ADK pipeline, we can yield UI status messages back to the user instantly (e.g., *"📡 Connecting to Strava..."*).
3. **Triggering ADK (Step 2):** We package the `message`, `activity_id`, and the newly fetched `analysis_data` into a JSON string. We hand this string to `self.runner.run_async`.
4. **Inside the Pipeline (`_run_async_impl`):** 
   - Unpacks the JSON `payload` to get the variables.
   - Fetches long-term athlete facts from the Vertex AI Memory Bank.
   - Hands off execution to `_run_pipeline`.

### Execution Paths inside `_run_pipeline`:

**Path A: Initial Activity Analysis**
- *Condition:* User sent a new `activity_id` and we successfully fetched `analysis_data` from Strava.
- *Action:* It saves the `analysis_data` into the session state (`ctx.session.state["last_activity"]`).
- *Action:* Calls `build_analyst_prompt` to format the data cleanly.
- *Action:* Calls the LLM via `_stream_specialist` using the **Activity Analyst Prompt** and yields the coaching report.

**Path B: Follow-up Chat**
- *Condition:* The user just typed a chat message (no new activity data was passed).
- *Action:* It reloads the last analyzed activity from session state (so the bot remembers what run you are talking about).
- *Action:* **Intent Routing:** It uses a quick LLM call (`_call_specialist` / ToolRouter) to check if the user is asking for external data (Dashboard, Race History, Goals). If so, it uses MCP tools to fetch that data and inject it into the prompt.
- *Action:* It fetches the last 10 chat messages from BigQuery so the LLM remembers the immediate conversation.
- *Action:* It combines the Context (Memory + Previous Activity + Chat History + MCP Data) and sends it the LLM via `stream_specialist` using the **General Coach Prompt**.

### Conclusion
*"So basically we are having one agent which will eventually collect details and passing to LLM and get the output of analysis."*

**Yes, absolutely.** The `CoachingPipeline` acts as an orchestrator. It acts as the brain that:
1. Gathers context (Strava Data, Memory, BigQuery History, MCP Server Data).
2. Assembles it into a massively enriched prompt.
3. Streams that final prompt to the Gemini LLM for the actual "thinking" and "generating".
