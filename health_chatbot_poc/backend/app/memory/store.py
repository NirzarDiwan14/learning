"""Long-term user memory with mem0 (pgvector in the same Postgres).

mem0 extracts durable facts from conversations ("prefers morning runs", "sore left calf
this week", "travelling to Lisbon on the 20th"), dedupes/updates them, and lets us search
them semantically. Users can view/add/delete them (like WHOOP "My Memory" / Oura "Memories").
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

from backend.app.config import get_settings

os.environ.setdefault("MEM0_TELEMETRY", "False")
log = logging.getLogger(__name__)

_FACT_INSTRUCTIONS = """Extract durable facts about the athlete that would help a coach later:
goals, preferences (training times, sports, foods), injuries/niggles and how they feel, schedule
changes, travel, life stress, equipment. Ignore numbers that come from wearable data (those are
stored elsewhere) and ignore generic chit-chat."""


@lru_cache
def get_memory():
    """Return a mem0 Memory instance, or None if no OpenAI key is configured."""
    s = get_settings()
    if not s.openai_api_key:
        log.warning("OPENAI_API_KEY missing - long-term memory disabled")
        return None
    from mem0 import Memory

    Path(s.mem0_history_db_path).parent.mkdir(parents=True, exist_ok=True)
    config = {
        "vector_store": {
            "provider": "pgvector",
            "config": {
                "connection_string": s.database_url,
                "collection_name": s.mem0_collection,
                "embedding_model_dims": s.embedding_dims,
            },
        },
        "llm": {"provider": "openai", "config": {"model": s.small_model, "api_key": s.openai_api_key}},
        "embedder": {"provider": "openai", "config": {"model": s.embedding_model, "api_key": s.openai_api_key,
                                                       "embedding_dims": s.embedding_dims}},
        "history_db_path": s.mem0_history_db_path,
        "custom_instructions": _FACT_INSTRUCTIONS,
    }
    return Memory.from_config(config)


def search_memories(user_id: str, query: str, k: int = 5) -> list[str]:
    m = get_memory()
    if not m:
        return []
    try:
        res = m.search(query, filters={"user_id": user_id}, top_k=k, threshold=0.3)
        return [r["memory"] for r in res.get("results", [])]
    except Exception:  # memory must never break a chat turn
        log.exception("mem0 search failed")
        return []


def remember_turn(user_id: str, user_msg: str, assistant_msg: str) -> None:
    """Run in a background task after the response is sent (keeps it off the latency path)."""
    m = get_memory()
    if not m:
        return
    try:
        m.add([{"role": "user", "content": user_msg}, {"role": "assistant", "content": assistant_msg}], user_id=user_id)
    except Exception:
        log.exception("mem0 add failed")


def list_memories(user_id: str) -> list[dict]:
    m = get_memory()
    if not m:
        return []
    res = m.get_all(filters={"user_id": user_id}, top_k=100)
    return [{"id": r["id"], "memory": r["memory"], "updated_at": r.get("updated_at") or r.get("created_at")}
            for r in res.get("results", [])]


def add_memory(user_id: str, text: str) -> None:
    m = get_memory()
    if m:
        m.add(text, user_id=user_id, infer=False)


def delete_memory(memory_id: str) -> None:
    m = get_memory()
    if m:
        m.delete(memory_id)
