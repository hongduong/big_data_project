# stocklake/dashboard:dev — Streamlit, reads curated data over WebHDFS.
# Build context = repo root.
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 1000 --shell /bin/bash app

# stocklake wheel with the [dashboard] extra (no pyspark).
COPY pyproject.toml README.md /tmp/build/
COPY src /tmp/build/src
RUN set -eux; \
    pip install "/tmp/build[dashboard]"; \
    rm -rf /tmp/build

WORKDIR /opt/stocklake
COPY dashboard /opt/stocklake/dashboard
COPY conf/app /opt/stocklake/conf/app
RUN chown -R app:app /opt/stocklake

ENV PYTHONPATH=/opt/stocklake/src \
    APP_CONF_DIR=/opt/stocklake/conf/app \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

RUN python -c "import stocklake.contracts, streamlit; print('stocklake + streamlit ok')"

USER app
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port", "8501", "--server.address", "0.0.0.0", "--server.headless", "true"]
