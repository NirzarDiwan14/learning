"""The four answer paths behind the router.

lookup / insight / coaching -> LangChain `create_agent` with a SMALL model and a narrow tool set
deep_analysis               -> Deep Agents harness (planning + `data-scientist` subagent that writes
                               and runs pandas code), LARGE model
"""

from __future__ import annotations

from functools import lru_cache

from deepagents import create_deep_agent
from langchain.agents import create_agent

from backend.app.agents import prompts
from backend.app.agents.context import HealthContext
from backend.app.agents.llm import large_model, small_model
from backend.app.tools.metrics_tools import ANALYST_TOOLS, COACHING_TOOLS, INSIGHT_TOOLS, LOOKUP_TOOLS
from backend.app.tools.sandbox import DEEP_ANALYSIS_TOOLS


@lru_cache
def lookup_agent():
    return create_agent(small_model(), LOOKUP_TOOLS, system_prompt=prompts.LOOKUP_PROMPT,
                        context_schema=HealthContext, name="lookup")


@lru_cache
def insight_agent():
    return create_agent(small_model(), INSIGHT_TOOLS, system_prompt=prompts.INSIGHT_PROMPT,
                        context_schema=HealthContext, name="insight")


@lru_cache
def coaching_agent():
    return create_agent(small_model(), COACHING_TOOLS, system_prompt=prompts.COACHING_PROMPT,
                        context_schema=HealthContext, name="coaching")


@lru_cache
def analyst_agent():
    data_scientist = {
        "name": "data-scientist",
        "description": (
            "Writes and executes pandas code over the user's daily metrics (df) and events to compute "
            "correlations, lagged effects, grouped or custom-window statistics. Give it one precise analysis request."
        ),
        "system_prompt": prompts.DATA_SCIENTIST_PROMPT,
        "tools": DEEP_ANALYSIS_TOOLS,
        "model": large_model(),
    }
    return create_deep_agent(
        model=large_model(),
        tools=ANALYST_TOOLS,
        system_prompt=prompts.ANALYST_PROMPT,
        subagents=[data_scientist],
        context_schema=HealthContext,
        name="analyst",
    )


PATHS = {
    "lookup": lookup_agent,
    "insight": insight_agent,
    "coaching": coaching_agent,
    "deep_analysis": analyst_agent,
}
