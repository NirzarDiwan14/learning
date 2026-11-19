"""FastAPI entrypoint.

Run locally (from health_chatbot_poc/):  uv run uvicorn backend.app.main:app --reload --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.api.routes import router
from backend.app.config import get_settings
from backend.app.db.connection import fetch_one, get_pool, init_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("health_poc")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_schema()
    if not fetch_one("SELECT 1 AS x FROM users LIMIT 1"):
        from backend.app.ingest.seed import seed

        log.info("Empty database - seeding synthetic multi-vendor data...")
        log.info("Seed summary: %s", seed())
    if get_settings().openai_api_key:
        from backend.app.agents.graph import get_graph

        get_graph()  # compile graph + set up the Postgres checkpointer tables
    else:
        log.warning("OPENAI_API_KEY not set - data endpoints work, chat/brief/memory return 503")
    yield
    get_pool().close()


app = FastAPI(title="Health Coach POC", version="0.1.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health")
def health():
    fetch_one("SELECT 1 AS ok")
    return {"status": "ok", "llm_configured": bool(get_settings().openai_api_key)}
