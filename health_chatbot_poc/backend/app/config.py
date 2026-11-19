"""Central settings, loaded from health_chatbot_poc/.env (or real env vars in Docker)."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

POC_ROOT = Path(__file__).resolve().parents[2]  # health_chatbot_poc/


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=POC_ROOT / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- LLM ---
    openai_api_key: str = ""
    small_model: str = "gpt-5.4-mini"  # router, lookups, insights, coaching, validator
    large_model: str = "gpt-5.5"  # deep-analysis path only
    embedding_model: str = "text-embedding-3-small"
    embedding_dims: int = 1536

    # --- Postgres ---
    postgres_host: str = "localhost"
    postgres_port: int = 5434
    postgres_db: str = "health_poc"
    postgres_user: str = "health"
    postgres_password: str = "health"

    # --- mem0 ---
    mem0_collection: str = "user_memories"
    mem0_history_db_path: str = str(POC_ROOT / ".mem0" / "history.db")

    # --- Behaviour ---
    history_turns: int = 6  # previous chat turns passed to the path agents
    sandbox_timeout_s: int = 20

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
