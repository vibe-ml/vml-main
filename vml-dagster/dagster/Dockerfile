FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --create-home dagster \
    && mkdir -p /opt/dagster/home /opt/dagster/storage \
    && chown -R dagster:dagster /opt/dagster
USER dagster
