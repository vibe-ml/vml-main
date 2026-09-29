FROM ghcr.io/astral-sh/uv:0.12.18 AS uv
FROM python:3.14-slim

COPY --from=uv /uv /usr/local/bin/uv
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    DAGSTER_HOME=/opt/dagster/home \
    PATH=/opt/openalex/.venv/bin:$PATH
WORKDIR /opt/openalex
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project \
    && useradd --uid 10001 --create-home dagster \
    && mkdir -p /opt/dagster/home /opt/dagster/storage \
    && chown -R dagster:dagster /opt/dagster
COPY src ./src
COPY alembic.ini ./
RUN chown -R dagster:dagster /opt/openalex
USER dagster
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.ingestion.defs"]
