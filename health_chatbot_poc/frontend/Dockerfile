# Build context = repo root (uses the root pyproject.toml + uv.lock). See docker-compose.yml.
FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.21 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project --no-dev

COPY health_chatbot_poc/frontend ./health_chatbot_poc/frontend
WORKDIR /app/health_chatbot_poc/frontend
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
