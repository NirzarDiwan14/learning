# Build context = repo root (uses the root pyproject.toml + uv.lock). See docker-compose.yml.
FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.21 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1 MEM0_TELEMETRY=False
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project --no-dev

COPY health_chatbot_poc/backend ./health_chatbot_poc/backend
WORKDIR /app/health_chatbot_poc
EXPOSE 8000
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
