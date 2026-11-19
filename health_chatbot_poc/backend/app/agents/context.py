"""Per-run context passed to every agent/tool via `context=` (LangChain ToolRuntime).

The LLM never chooses the user id - tools read it from the runtime context, so one
user's tools can never be pointed at another user's data.
"""

from dataclasses import dataclass


@dataclass
class HealthContext:
    user_id: str
