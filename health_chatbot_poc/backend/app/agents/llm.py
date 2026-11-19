"""Model tiering: a small, cheap model for most paths; a large model only for deep analysis."""

from functools import lru_cache

from langchain_openai import ChatOpenAI

from backend.app.config import get_settings


@lru_cache
def small_model() -> ChatOpenAI:
    s = get_settings()
    return ChatOpenAI(model=s.small_model, api_key=s.openai_api_key, timeout=60, max_retries=2)


@lru_cache
def large_model() -> ChatOpenAI:
    s = get_settings()
    return ChatOpenAI(model=s.large_model, api_key=s.openai_api_key, timeout=180, max_retries=2)
