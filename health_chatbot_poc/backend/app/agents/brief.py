"""Proactive daily brief: one small-model call per user per day, cached in Postgres.

This is the "precompute the common answers" pattern (Strava post-upload insights, WHOOP/Oura
check-ins): most users read the brief instead of asking, and it costs one call per day.
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

from langchain_core.messages import HumanMessage, SystemMessage

from backend.app.agents import prompts
from backend.app.agents.context import HealthContext
from backend.app.agents.llm import small_model
from backend.app.agents.validator import check_grounding
from backend.app.db.connection import conn, fetch_one
from backend.app.tools.metrics_tools import get_events, get_today_snapshot, get_trends


def _rt(user_id: str):
    # Call the tool functions directly (no LLM) with a minimal runtime carrying the context.
    return SimpleNamespace(context=HealthContext(user_id=user_id))


def get_daily_brief(user_id: str, force: bool = False) -> dict:
    today = date.today()
    if not force:
        row = fetch_one("SELECT content, created_at FROM daily_briefs WHERE user_id=%s AND day=%s", (user_id, today))
        if row:
            return {"brief": row["content"], "cached": True, "created_at": row["created_at"]}

    rt = _rt(user_id)
    facts = {
        "today": json.loads(get_today_snapshot.func(runtime=rt)),
        "trends_30d": json.loads(get_trends.func(runtime=rt, days=30, metrics=["hrv_rmssd", "resting_hr", "sleep_duration", "training_load"])),
        "upcoming_events": json.loads(get_events.func(runtime=rt, direction="upcoming", days=30)),
    }
    facts_json = json.dumps(facts, default=str)
    text = small_model().invoke([SystemMessage(prompts.BRIEF_PROMPT), HumanMessage(facts_json)]).text
    validation = check_grounding(text, [facts_json])
    with conn() as c:
        c.execute(
            """INSERT INTO daily_briefs (user_id, day, content) VALUES (%s,%s,%s)
               ON CONFLICT (user_id, day) DO UPDATE SET content = EXCLUDED.content, created_at = now()""",
            (user_id, today, text),
        )
    return {"brief": text, "cached": False, "validation": validation}
