# stocklake/collector:dev — batch collectors, Kafka producers, replay simulator.
# Build context = repo root.
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 1000 --shell /bin/bash app

# stocklake wheel with the [collector] extra (no pyspark).
COPY pyproject.toml README.md /tmp/build/
COPY src /tmp/build/src
RUN set -eux; \
    pip install "/tmp/build[collector]"; \
    rm -rf /tmp/build

WORKDIR /opt/stocklake
COPY jobs /opt/stocklake/jobs
COPY conf/app /opt/stocklake/conf/app

# collector_state volume is mounted here; the directory must exist and belong to `app`
# so the named volume inherits that ownership.
RUN mkdir -p /opt/stocklake/state && chown -R app:app /opt/stocklake

# Dev: ./src is bind-mounted at /opt/stocklake/src and wins over the installed wheel.
ENV PYTHONPATH=/opt/stocklake/src \
    APP_CONF_DIR=/opt/stocklake/conf/app \
    COLLECTOR_STATE_DIR=/opt/stocklake/state

RUN python -c "import stocklake.contracts; print('stocklake ok')"

USER app
# No scheduler yet: keep the container alive; run jobs with `docker compose exec collector ...`.
CMD ["sleep", "infinity"]
