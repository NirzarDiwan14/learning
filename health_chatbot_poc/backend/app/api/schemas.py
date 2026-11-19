from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    user_id: str
    thread_id: str = Field(description="Conversation id; history is persisted per (user, thread)")
    message: str = Field(min_length=1, max_length=4000)


class ChatResponse(BaseModel):
    answer: str
    intent: str
    route_reason: str
    tool_calls: list[dict]
    validation: dict
    memories_used: list[str]
    usage: dict
    latency_ms: int


class MemoryIn(BaseModel):
    text: str = Field(min_length=2, max_length=500)
