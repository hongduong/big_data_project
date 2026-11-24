# stocklake/spark:dev — one image for spark-master, spark-worker1..3 and spark-driver,
# so executors run exactly the same Python + stocklake code as the driver.
# Build context = repo root:  docker compose build   (or: make build)
#
# Facts about the base image (checked on the PC, 24/11/2026):
#   Ubuntu 22.04, Java 17, /usr/bin/python3 = Python 3.10.12, pip 22.0.2,
#   SPARK_HOME=/opt/spark, user spark (uid 185), entrypoint /opt/entrypoint.sh
#   (passes non-k8s commands straight through), $SPARK_HOME/conf does not exist,
#   `import pyspark` FAILS in plain python3 (pyspark only lives in $SPARK_HOME/python).

ARG SPARK_IMAGE_BASE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE_BASE}

USER root

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 1) Kafka connector jars, baked in so spark-submit needs no internet.
#    Resolve with Ivy for the exact Spark version of this image (read from spark-core jar),
#    then copy only the 4 jars Spark does not already ship. Transitive deps that Ivy also
#    pulls (hadoop-client-*, lz4, snappy, slf4j, jsr305, commons-logging) are identical
#    versions already in $SPARK_HOME/jars, so they are not copied.
RUN set -eux; \
    SPARK_VERSION="$(ls "${SPARK_HOME}"/jars/spark-core_2.12-*.jar | sed -E 's|.*/spark-core_2\.12-(.*)\.jar|\1|')"; \
    echo "Spark version: ${SPARK_VERSION}"; \
    echo 'print("ivy resolve done")' > /tmp/noop.py; \
    "${SPARK_HOME}/bin/spark-submit" --master 'local[1]' \
        --conf spark.jars.ivy=/tmp/ivy \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}" \
        /tmp/noop.py; \
    for a in spark-sql-kafka-0-10_2.12 spark-token-provider-kafka-0-10_2.12 kafka-clients commons-pool2; do \
        set -- /tmp/ivy/jars/*_"${a}"-[0-9]*.jar; \
        [ "$#" -eq 1 ] && [ -f "$1" ] || { echo "expected exactly one jar for ${a}: $*"; exit 1; }; \
        base="$(basename "$1")"; \
        cp "$1" "${SPARK_HOME}/jars/${base#*_}"; \
    done; \
    ls "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    rm -rf /tmp/ivy /tmp/noop.py /root/.ivy2

# 2) Make pyspark + py4j importable from plain python3 (driver scripts, tests, healthcheck)
#    via a .pth file, so PYTHONPATH stays free for the dev-mounted source tree.
RUN set -eux; \
    SITE="$(python3 -c 'import site; print(site.getsitepackages()[0])')"; \
    mkdir -p "${SITE}"; \
    PY4J="$(ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip)"; \
    printf '%s\n%s\n' "${SPARK_HOME}/python" "${PY4J}" > "${SITE}/spark.pth"; \
    cat "${SITE}/spark.pth"

# 3) Spark + Hadoop client configuration.
COPY conf/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY conf/hadoop/core-site.xml conf/hadoop/hdfs-site.xml /opt/stocklake/conf/hadoop/

# 4) stocklake wheel with the [spark] extra (pyarrow, numpy; pyspark comes from the image).
#    Ubuntu's pip 22.0.2 builds this PEP 621 project as "UNKNOWN 0.0.0" (Debian-patched
#    install scheme, pypa/setuptools#3269), so upgrade pip first.
COPY pyproject.toml README.md /tmp/build/
COPY src /tmp/build/src
RUN set -eux; \
    python3 -m pip install --upgrade pip; \
    python3 -m pip install "/tmp/build[spark]"; \
    rm -rf /tmp/build

# 5) Job entry points and app config.
COPY jobs /opt/stocklake/jobs
COPY conf/app /opt/stocklake/conf/app

# /opt/stocklake/src does not exist in the image. In development docker-compose bind-mounts
# ./src there; PYTHONPATH puts it ahead of site-packages so the mounted code wins over the
# installed wheel on driver AND executors (Spark appends the worker's PYTHONPATH for
# Python workers).
ENV PYTHONPATH=/opt/stocklake/src \
    APP_CONF_DIR=/opt/stocklake/conf/app \
    HADOOP_CONF_DIR=/opt/stocklake/conf/hadoop \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
    PATH=${PATH}:/opt/spark/bin:/opt/spark/sbin

RUN set -eux; \
    chown -R spark:spark "${SPARK_HOME}/conf" /opt/stocklake; \
    python3 -c "import pyspark, pyarrow, numpy, stocklake; print('pyspark', pyspark.__version__, '| stocklake', stocklake.__file__)"

USER spark
WORKDIR /opt/spark/work-dir
