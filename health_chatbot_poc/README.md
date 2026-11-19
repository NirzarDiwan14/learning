# Health coach chatbot — POC

A proof of concept of the architecture in [`../healthcare_chatbot_llm_research.md`](../healthcare_chatbot_llm_research.md):
**the LLM never sees raw wearable data**. Code computes the numbers ahead of time, typed tools serve
small JSON summaries, a router picks the cheapest path that can answer, and a validator checks that
every number in an answer came from a tool.

```
 WHOOP / Oura / Apple Watch / Polar payloads (synthetic, each in its own shape and units)
            │
 [1] INGEST + NORMALIZE ──► Postgres `samples`, `workouts` (tagged by source)
            │
 [2] PRECOMPUTE "Health Facts" (pandas) ──► daily rollups · monthly aggregates · 7/30/90-day baselines
            │                                z-scores · trends · anomalies · change-points · readiness
 [3] METRICS API / SEMANTIC LAYER ──► typed LangChain tools (user id from runtime context)
            │
 [4] ROUTER (small model, structured output) + red-flag safety check
            │
   ┌────────┼───────────┬─────────────────┐
 lookup   insight     coaching       deep_analysis
 (create_agent, small model)         (Deep Agent, large model → `data-scientist`
                                      subagent writes pandas, runs in a sandbox)
            │
 [5] CONTEXT: profile card + mem0 memories (pgvector)
 [6] ANSWER  ──► [7] VALIDATOR (numbers traced to tool output, 1 repair pass)
            │
 mem0 fact extraction runs in a FastAPI background task, after the response is sent
```

## Stack

| Concern | Choice |
|---|---|
| Orchestration | LangGraph `StateGraph` (router → path → validator), `PostgresSaver` for chat history |
| Path agents | LangChain `create_agent` (lookup / insight / coaching) |
| Deep analysis | Deep Agents `create_deep_agent` with a `data-scientist` subagent |
| LLM | `langchain-openai` (`SMALL_MODEL`, `LARGE_MODEL` in `.env`) |
| Long-term memory | mem0, stored in pgvector in the same Postgres |
| Database | Postgres 17 + pgvector (`pgvector/pgvector:pg17`) on **5434** |
| API | FastAPI |
| UI | Streamlit (login: `admin@gmail.com` / `admin`) |
| Dependencies | the **root** `pyproject.toml` / `uv.lock` |

## Run it

### 1. Configure

```bash
cd health_chatbot_poc
cp .env.example .env        # then set OPENAI_API_KEY
```

Without a key, the dashboard and data endpoints still work. Chat, the daily brief and memory return 503.

### 2a. Everything in Docker

```bash
docker compose up -d --build
# UI:  http://localhost:8501      API docs: http://localhost:8000/docs
```

If a host port is taken, change `POSTGRES_HOST_PORT`, `BACKEND_HOST_PORT` or `FRONTEND_HOST_PORT` in `.env`.
On this machine, 5434 and 8000 were already in use by other processes while this was built.

### 2b. Local dev (Postgres in Docker, apps with uv)

```bash
docker compose up -d postgres
uv run uvicorn backend.app.main:app --reload --port 8000          # from health_chatbot_poc/
cd frontend && uv run streamlit run streamlit_app.py              # from health_chatbot_poc/frontend/
```

On first start, the backend creates the schema and seeds two athletes: Aarav (marathon, Achilles history,
flu dip, race in 18 days, poor sleep this week) and Maya (football, no Oura ring, an away tournament
with poor sleep). The data is 180 days ending *today*. Use **Reseed demo data** in the sidebar (or
`POST /admin/reseed`) to roll it forward on a later day.

## Things to try

| Question | Path | What to look for in the trace |
|---|---|---|
| "When is my next event?" | lookup | 1 tool call, small model |
| "Suggest exercises for today and the next 3 days" | coaching | readiness tier → intensity cap; the Achilles-contraindicated hill sprints are excluded |
| "Give me the ups and downs of my last 6 months" | insight | `get_trends` + past events; illness and travel show up as anomaly episodes |
| "Does a hard training day lower my HRV the next morning?" | deep_analysis | `task` → data-scientist → `run_python_analysis` |
| (as Maya) "What's my Oura sleep score trend?" | lookup | explicit "metric not tracked" (she has no Oura), not a guessed number |
| "I had chest pain on my run" | safety | fixed response, no LLM call |
| "I prefer morning runs and my left calf is tight" | any | appears under **My memory** a few seconds later |

Each answer has an expandable trace showing the route and its reason, the tool calls, the validator result,
tokens used per model, and latency.

## Layout

```
health_chatbot_poc/
├── docker-compose.yml          postgres(pgvector) + backend + frontend
├── .env.example                credentials and config (copy to .env)
├── backend/
│   ├── Dockerfile              build context = repo root (root pyproject/uv.lock)
│   └── app/
│       ├── main.py             FastAPI app, lifespan: schema + seed + graph
│       ├── config.py           pydantic-settings (.env)
│       ├── db/                 schema.sql, psycopg pool
│       ├── ingest/             vendor payload generator, normalizer, seed, exercise library
│       ├── facts/precompute.py [2] Health Facts (all maths happens here)
│       ├── tools/              [3] catalog + metric tools, sandboxed pandas tool
│       ├── agents/             [4]-[7] graph, router, paths, prompts, validator, daily brief
│       ├── memory/store.py     mem0 (pgvector)
│       └── api/                routes + schemas
└── frontend/
    ├── streamlit_app.py        login gate, sidebar, st.navigation
    ├── app_pages/              chat · dashboard · memory · how it works
    └── lib/                    API client, POC auth
```

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/chat` | `{user_id, thread_id, message}` → answer, intent, tool calls, validation, usage, latency |
| GET | `/chat/{user_id}/{thread_id}` | persisted conversation |
| GET | `/users`, `/users/{id}/profile`, `/users/{id}/series`, `/users/{id}/data-footprint` | dashboard data (UI only, never sent to the LLM) |
| GET | `/users/{id}/brief?force=` | daily brief, cached once per day |
| GET/POST/DELETE | `/users/{id}/memories[/{mid}]` | view / add / delete long-term memories |
| POST | `/admin/reseed` | regenerate demo data |

## What's POC-grade (change these before production)

- **Auth:** one hard-coded admin login in Streamlit; the API has no auth.
- **Code sandbox:** a subprocess with no environment variables, rlimits and a timeout. Use a real sandbox (container, gVisor or Firecracker, with no network) in production.
- **Validator:** regex number matching with a ±2% tolerance. It catches invented or converted numbers, but it can't check the reasoning.
- **Data:** synthetic vendor payloads. Real ingestion needs OAuth with each vendor, webhooks and incremental precompute.
- **Precompute:** runs in-process at seed time. In production it should be a nightly job plus an event-driven job on new data (e.g. a Kafka consumer per user).
- **Responses:** not streamed. LangGraph `stream_mode="messages"` combined with `st.write_stream` is the next step.
