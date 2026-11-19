"""Top-level orchestration graph (LangGraph).

  START -> load_context -> route -+-> lookup ---------+
                                  +-> insight --------+
                                  +-> coaching -------+--> validate -> END
                                  +-> deep_analysis --+
                                  +-> safety ---------------------> END

Conversation history is persisted per thread in Postgres (PostgresSaver). Path agents
are stateless; they receive the last few turns + a compact <user_context> block.
"""

from __future__ import annotations

import logging
from datetime import date
from functools import lru_cache
from typing import Annotated, Literal

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from backend.app.agents import prompts
from backend.app.agents.context import HealthContext
from backend.app.agents.llm import small_model
from backend.app.agents.paths import PATHS
from backend.app.agents.validator import RED_FLAGS, SAFETY_MESSAGE, check_grounding
from backend.app.config import get_settings
from backend.app.db.connection import fetch_all, fetch_one, get_pool
from backend.app.memory.store import search_memories

log = logging.getLogger(__name__)

Intent = Literal["lookup", "insight", "coaching", "deep_analysis"]


class ChatState(TypedDict, total=False):
    messages: Annotated[list[AnyMessage], add_messages]  # persisted conversation (human + final AI)
    user_id: str
    question: str
    user_context: str
    memories: list[str]
    intent: str
    route_reason: str
    draft: str
    evidence: list[str]
    trace: list[dict]
    validation: dict


class Route(BaseModel):
    intent: Intent = Field(description="Which path should answer")
    reason: str = Field(description="One short sentence explaining the choice")


# ---------------------------------------------------------------- context assembly [5]
def build_profile_card(user_id: str) -> str:
    u = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not u:
        raise ValueError(f"unknown user {user_id}")
    rd = fetch_one("SELECT tier, score FROM readiness WHERE user_id=%s", (user_id,))
    ev = fetch_all(
        "SELECT event_date, title, event_type FROM events WHERE user_id=%s AND event_date >= CURRENT_DATE ORDER BY event_date LIMIT 2",
        (user_id,),
    )
    today = date.today()
    lines = [
        f"today: {today.isoformat()} ({today.strftime('%A')})",
        f"athlete: {u['name']}, {u['age']}y, {u['sex']}, sport: {u['sport']}",
        f"goals: {u['goals']}",
        f"injuries/notes: {u['injuries']}",
        f"devices: {', '.join(u['devices'])}",
    ]
    if rd:
        lines.append(f"readiness today: {rd['tier']} ({rd['score']}/100)")
    for e in ev:
        lines.append(f"upcoming: {e['title']} ({e['event_type']}) on {e['event_date']} - in {(e['event_date'] - today).days} days")
    return "\n".join(lines)


def load_context(state: ChatState) -> dict:
    question = state["messages"][-1].text
    card = build_profile_card(state["user_id"])
    memories = search_memories(state["user_id"], question)
    ctx = card + ("\nremembered about the user:\n- " + "\n- ".join(memories) if memories else "")
    # reset per-turn fields (non-reducer fields would otherwise carry over from the last turn)
    return {"question": question, "user_context": ctx, "memories": memories, "evidence": [], "trace": [],
            "draft": "", "validation": {}, "intent": "", "route_reason": ""}


# ---------------------------------------------------------------- router [4]
def route(state: ChatState) -> dict:
    if RED_FLAGS.search(state["question"]):
        return {"intent": "safety", "route_reason": "Red-flag symptom keywords detected - deterministic safety response."}
    history = state["messages"][:-1][-4:]
    decision: Route = small_model().with_structured_output(Route).invoke(
        [SystemMessage(prompts.ROUTER_PROMPT), *history, HumanMessage(state["question"])]
    )
    return {"intent": decision.intent, "route_reason": decision.reason}


def pick_path(state: ChatState) -> str:
    return state["intent"]


# ---------------------------------------------------------------- paths
def _make_path_node(name: str):
    def node(state: ChatState) -> dict:
        agent = PATHS[name]()
        n_hist = get_settings().history_turns * 2
        history = state["messages"][:-1][-n_hist:]
        user_turn = HumanMessage(f"<user_context>\n{state['user_context']}\n</user_context>\n\n{state['question']}")
        inp = [*history, user_turn]
        result = agent.invoke(
            {"messages": inp},
            context=HealthContext(user_id=state["user_id"]),
            config={"recursion_limit": 60 if name == "deep_analysis" else 15},
        )
        new = result["messages"][len(inp):]
        evidence = [m.text for m in new if isinstance(m, ToolMessage)]
        trace = [{"tool": tc["name"], "args": tc["args"]} for m in new if isinstance(m, AIMessage) for tc in m.tool_calls]
        return {"draft": new[-1].text if new else "", "evidence": evidence, "trace": trace}

    node.__name__ = name
    return node


def safety(state: ChatState) -> dict:
    return {"messages": [AIMessage(SAFETY_MESSAGE)], "validation": {"skipped": "safety"}, "draft": SAFETY_MESSAGE}


# ---------------------------------------------------------------- validator [7]
def validate(state: ChatState) -> dict:
    draft = state["draft"] or "Sorry, I couldn't produce an answer."
    evidence = [*state.get("evidence", []), state["user_context"], state["question"]]
    result = check_grounding(draft, evidence)
    repaired = False
    if result["unverified"]:
        ev_text = "\n---\n".join(state.get("evidence", []))[:12000]
        fixed = small_model().invoke([
            SystemMessage(prompts.VALIDATOR_REPAIR_PROMPT),
            HumanMessage(f"EVIDENCE:\n{ev_text}\n\n<user_context>\n{state['user_context']}\n</user_context>\n\n"
                         f"UNVERIFIED NUMBERS: {result['unverified']}\n\nDRAFT:\n{draft}"),
        ]).text
        after = check_grounding(fixed, evidence)
        if len(after["unverified"]) < len(result["unverified"]):
            draft, repaired = fixed, True
            result = {**after, "unverified_before_repair": result["unverified"]}
    result["repaired"] = repaired
    return {"messages": [AIMessage(draft)], "validation": result}


# ---------------------------------------------------------------- graph
@lru_cache
def get_checkpointer() -> PostgresSaver:
    saver = PostgresSaver(get_pool())
    saver.setup()
    return saver


@lru_cache
def get_graph():
    b = StateGraph(ChatState)
    b.add_node("load_context", load_context)
    b.add_node("route", route)
    for name in PATHS:
        b.add_node(name, _make_path_node(name))
        b.add_edge(name, "validate")
    b.add_node("safety", safety)
    b.add_node("validate", validate)
    b.add_edge(START, "load_context")
    b.add_edge("load_context", "route")
    b.add_conditional_edges("route", pick_path, [*PATHS, "safety"])
    b.add_edge("safety", END)
    b.add_edge("validate", END)
    return b.compile(checkpointer=get_checkpointer())
