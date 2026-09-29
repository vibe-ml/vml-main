FROM ghcr.io/astral-sh/uv:0.12.18 AS uv
FROM python:3.14-slim

COPY --from=uv /uv /usr/local/bin/uv
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    DAGSTER_HOME=/opt/dagster/home \
    PATH=/opt/bertopic/.venv/bin:$PATH
WORKDIR /opt/bertopic
COPY pyproject.toml uv.lock ./
# Skip the locked CUDA torch. Its NVIDIA libraries expand past the free space
# on the vml.pub root disk, and topic fitting runs on CPU.
RUN uv sync --frozen --no-dev --no-install-project \
      --no-install-package torch \
      --no-install-package triton \
      --no-install-package cuda-bindings \
      --no-install-package cuda-pathfinder \
      --no-install-package cuda-toolkit \
      --no-install-package nvidia-cublas \
      --no-install-package nvidia-cuda-cupti \
      --no-install-package nvidia-cuda-nvrtc \
      --no-install-package nvidia-cuda-runtime \
      --no-install-package nvidia-cudnn-cu13 \
      --no-install-package nvidia-cufft \
      --no-install-package nvidia-cufile \
      --no-install-package nvidia-curand \
      --no-install-package nvidia-cusolver \
      --no-install-package nvidia-cusparse \
      --no-install-package nvidia-cusparselt-cu13 \
      --no-install-package nvidia-nccl-cu13 \
      --no-install-package nvidia-nvjitlink \
      --no-install-package nvidia-nvshmem-cu13 \
      --no-install-package nvidia-nvtx \
    && uv pip install --python /opt/bertopic/.venv/bin/python \
         "torch==2.14.0+cpu" \
         --index-url https://download.pytorch.org/whl/cpu \
         --no-deps \
    && .venv/bin/python -c "import dagster_postgres, dagster_docker; from sqlalchemy import create_engine; assert create_engine('postgresql://u:p@h/db').dialect.driver == 'psycopg2'" \
    && useradd --uid 10001 --create-home dagster \
    && mkdir -p /opt/dagster/home /opt/dagster/storage \
    && chown -R dagster:dagster /opt/dagster
ENV PYTHONPATH=/opt/bertopic
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
RUN chown -R dagster:dagster /opt/bertopic
USER dagster
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.orchestration.defs"]
