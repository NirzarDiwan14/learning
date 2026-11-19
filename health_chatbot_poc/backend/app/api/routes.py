from __future__ import annotations

import time

from fastapi import APIRouter, BackgroundTasks, HTTPException
from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.messages import HumanMessage

from backend.app.agents.brief import get_daily_brief
from backend.app.agents.graph import build_profile_card, get_graph
from backend.app.api.schemas import ChatRequest, ChatResponse, MemoryIn
from backend.app.config import get_settings
from backend.app.db.connection import fetch_all, fetch_one
from backend.app.memory import store
from backend.app.tools.catalog import METRICS

router = APIRouter()


def _require_user(user_id: str) -> dict:
    u = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not u:
        raise HTTPException(404, f"Unknown user '{user_id}'")
    return u


def _require_llm() -> None:
    if not get_settings().openai_api_key:
        raise HTTPException(503, "OPENAI_API_KEY is not set in health_chatbot_poc/.env")


# ------------------------------------------------------------------ users / dashboard
@router.get("/users")
def list_users():
    return fetch_all(
        """SELECT u.id, u.name, u.sport, u.devices, r.tier, r.score FROM users u
           LEFT JOIN readiness r ON r.user_id = u.id ORDER BY u.name"""
    )


@router.get("/users/{user_id}/profile")
def profile(user_id: str):
    u = _require_user(user_id)
    return {
        "user": u,
        "readiness": fetch_one("SELECT as_of, tier, score, drivers FROM readiness WHERE user_id=%s", (user_id,)),
        "events": fetch_all("SELECT event_date, title, event_type, notes FROM events WHERE user_id=%s ORDER BY event_date", (user_id,)),
        "baselines": fetch_all("SELECT * FROM metric_baselines WHERE user_id=%s ORDER BY metric", (user_id,)),
        "profile_card": build_profile_card(user_id),
    }


@router.get("/users/{user_id}/series")
def series(user_id: str, metrics: str = "hrv_rmssd,resting_hr,sleep_duration", days: int = 180):
    """Daily series for the dashboard charts (UI only - this is never sent to the LLM)."""
    _require_user(user_id)
    wanted = [m for m in metrics.split(",") if m in METRICS]
    rows = fetch_all(
        "SELECT day, metric, value FROM daily_metrics WHERE user_id=%s AND metric = ANY(%s) AND day > CURRENT_DATE - %s::int ORDER BY day",
        (user_id, wanted, days),
    )
    return rows


@router.get("/users/{user_id}/data-footprint")
def footprint(user_id: str):
    """Shows the context-reduction the Health Facts layer buys (raw -> daily -> summaries)."""
    _require_user(user_id)
    q = lambda t: fetch_one(f"SELECT COUNT(*) n FROM {t} WHERE user_id=%s", (user_id,))["n"]  # noqa: E731
    return {"raw_samples": q("samples"), "workouts": q("workouts"), "daily_rows": q("daily_metrics"),
            "monthly_rows": q("metric_monthly"), "baselines": q("metric_baselines"),
            "anomalies": q("anomalies"), "change_points": q("change_points")}


@router.get("/users/{user_id}/brief")
def brief(user_id: str, force: bool = False):
    _require_user(user_id)
    _require_llm()
    return get_daily_brief(user_id, force=force)


# ------------------------------------------------------------------ chat
@router.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, background: BackgroundTasks):
    _require_user(req.user_id)
    _require_llm()
    t0 = time.perf_counter()
    config = {"configurable": {"thread_id": f"{req.user_id}:{req.thread_id}"}}
    with get_usage_metadata_callback() as cb:
        state = get_graph().invoke({"messages": [HumanMessage(req.message)], "user_id": req.user_id}, config=config)
    answer = state["messages"][-1].text
    # Long-term memory extraction happens AFTER the response is returned (off the latency path).
    background.add_task(store.remember_turn, req.user_id, req.message, answer)
    return ChatResponse(
        answer=answer,
        intent=state.get("intent", ""),
        route_reason=state.get("route_reason", ""),
        tool_calls=state.get("trace", []),
        validation=state.get("validation", {}),
        memories_used=state.get("memories", []),
        usage={model: dict(u) for model, u in cb.usage_metadata.items()},
        latency_ms=int((time.perf_counter() - t0) * 1000),
    )


@router.get("/chat/{user_id}/{thread_id}")
def chat_history(user_id: str, thread_id: str):
    snap = get_graph().get_state({"configurable": {"thread_id": f"{user_id}:{thread_id}"}})
    msgs = (snap.values or {}).get("messages", []) if snap else []
    return [{"role": "user" if m.type == "human" else "assistant", "content": m.text} for m in msgs]


# ------------------------------------------------------------------ long-term memory (mem0)
@router.get("/users/{user_id}/memories")
def get_memories(user_id: str):
    _require_user(user_id)
    return store.list_memories(user_id)


@router.post("/users/{user_id}/memories")
def post_memory(user_id: str, body: MemoryIn):
    _require_user(user_id)
    _require_llm()
    store.add_memory(user_id, body.text)
    return {"ok": True}


@router.delete("/users/{user_id}/memories/{memory_id}")
def remove_memory(user_id: str, memory_id: str):
    _require_user(user_id)
    store.delete_memory(memory_id)
    return {"ok": True}


# ------------------------------------------------------------------ admin
@router.post("/admin/reseed")
def reseed():
    from backend.app.ingest.seed import seed

    return seed()
