# stocklake — Makefile (run from the repo root on the PC).
# docker compose v2 syntax. Recipes use bash with -eu -o pipefail.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE     ?= docker compose
PROJECT     ?= $(or $(COMPOSE_PROJECT_NAME),stocklake)
# Host Python for lint/docs: the repo .venv when present (Mac), else python3 (PC).
PYTHON      ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
SPARK_IMAGE ?= stocklake/spark:dev
W           ?= 3
PYTEST_ARGS ?=
# Sample pipeline (e2e-sample and its steps).
INGEST_ARGS  ?=
REPLAY_DAYS  ?= 5
REPLAY_RATE  ?= 2000
REPLAY_GROUP ?= sample
REPLAY_ARGS  ?=
STREAM_ARGS  ?=
# Lát 2: minute history (HIST_GROUP sample|vn30, or SYMBOLS=FPT,VNM) and intraday DQ.
HIST_GROUP   ?= sample
SYMBOLS      ?=
HIST_ARGS    ?=
DQ_ARGS      ?=
# Intraday clean (incremental by default): CLEAN_ARGS=--full or CLEAN_ARGS='--dates 2026-11-20'.
CLEAN_ARGS   ?=
# Regroup existing raw/intraday_history files into one row group per month (no VCI calls).
REGROUP_ARGS ?=
SPARK_SUBMIT := docker exec spark-driver spark-submit
JOBS_DIR     := /opt/stocklake/jobs

# Containers expected by `make health` (workers are added below).
CORE_CONTAINERS := kafka namenode datanode1 datanode2 datanode3 spark-master spark-driver collector dashboard
ALL_WORKERS     := spark-worker1 spark-worker2 spark-worker3

# `make health W=n` checks workers 1..n; without W it checks the worker containers that exist
# (`make up W=n` removes workers above n, so existing = expected; a crashed one still counts).
ifeq ($(origin W),command line)
EXPECT_WORKERS := $(wordlist 1,$(W),$(ALL_WORKERS))
else
EXPECT_WORKERS :=
endif

.PHONY: help build up down health topics smoke-kafka smoke-spark test lint docs stats clean-all \
        ingest-daily ingest-intraday-history regroup-intraday-history replay stream-intraday-once \
        verify-replay etl-clean dq-intraday e2e-sample

help:
	@echo "Targets:"
	@echo "  build              build stocklake/spark, collector, dashboard images"
	@echo "  up [W=1|2|3]       start the stack with W Spark workers (default 3)"
	@echo "  down               stop and remove containers (volumes are kept)"
	@echo "  health [W=n]       container state/health, live DataNodes, HDFS safe mode, ALIVE Spark workers"
	@echo "  topics             create Kafka topics (idempotent)"
	@echo "  smoke-kafka        produce + consume one message on topic smoke_test"
	@echo "  smoke-spark        SparkPi + check executors ran on every registered worker"
	@echo "  test               pytest tests/unit (incl. spark-marked) inside $(SPARK_IMAGE)"
	@echo "  lint               ruff check"
	@echo "  docs               regenerate docs/data_dictionary.md"
	@echo "  stats              docker stats --no-stream for project containers"
	@echo "  ingest-daily       daily bars (sample symbols, ~1 year) -> /data/raw/daily_ohlcv [INGEST_ARGS=]"
	@echo "  ingest-intraday-history [HIST_GROUP=sample|vn30] [SYMBOLS=A,B] [HIST_ARGS=]"
	@echo "                     1-minute history (VCI, 1 req/s, refuses trading hours) -> raw/intraday_history"
	@echo "  regroup-intraday-history [REGROUP_ARGS=--dry-run|--symbols A,B|--years 2025]"
	@echo "                     rewrite existing history files as one row group per VN month (no VCI)"
	@echo "  replay [REPLAY_GROUP=sample|vn30]   replay REPLAY_DAYS days from raw/intraday_history to Kafka at REPLAY_RATE msg/s"
	@echo "  stream-intraday-once   Kafka intraday_prices -> /data/raw/intraday_prices (availableNow)"
	@echo "  verify-replay RUN_ID=.. EXPECTED=..   distinct raw records of a replay run == sent"
	@echo "  etl-clean [CLEAN_ARGS=--full|'--dates D ...']   raw -> clean for daily and intraday bars"
	@echo "                     (intraday is incremental: only months whose raw input changed)"
	@echo "  dq-intraday [DQ_ARGS=]   intraday_1m checks (day coverage, minute vs daily volume)"
	@echo "  e2e-sample         whole sample pipeline in order; fails if any step fails"
	@echo "  clean-all CONFIRM=yes   down + DELETE ALL VOLUMES (HDFS, Kafka, collector state)"

build:
	$(COMPOSE) --profile w3 build

up:
	@case "$(W)" in 1|2|3) ;; *) echo "W must be 1, 2 or 3 (got '$(W)')"; exit 2;; esac
	@# --wait: return only once every started container is healthy, so `make up && make health` works.
	$(COMPOSE) --profile w$(W) up -d --wait --wait-timeout 300
	@# Scaling down: remove workers above W so health/benchmarks see exactly W workers.
	@for i in 1 2 3; do \
	  if [ $$i -gt $(W) ] && docker ps -a -q --filter "name=^spark-worker$$i$$" | grep -q .; then \
	    echo "removing container spark-worker$$i (W=$(W))"; \
	    $(COMPOSE) --profile w3 rm --stop --force spark-worker$$i; \
	  fi; \
	done
	@# After a restart the NameNode stays in safe mode (read-only) until enough block reports
	@# arrive + 30 s extension; wait so jobs started right after `make up` can write.
	@echo "waiting for HDFS to leave safe mode"; \
	timeout 180 docker exec namenode hdfs dfsadmin -safemode wait 2>/dev/null

# Never removes volumes. --profile w3 so every worker container is included.
down:
	$(COMPOSE) --profile w3 down --remove-orphans

health:
	@fail=0; \
	workers="$(EXPECT_WORKERS)"; \
	if [ -z "$$workers" ]; then \
	  workers="$$(docker ps -a --format '{{.Names}}' | grep -E '^spark-worker[1-3]$$' | sort | tr '\n' ' ' || true)"; \
	  [ -n "$${workers// /}" ] || workers="$(ALL_WORKERS)"; \
	fi; \
	n_exp=$$(echo $$workers | wc -w); \
	printf '%-15s %-10s %s\n' CONTAINER STATE HEALTH; \
	for c in $(CORE_CONTAINERS) $$workers; do \
	  s="$$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' $$c 2>/dev/null || echo 'missing -')"; \
	  st="$${s%% *}"; hl="$${s##* }"; \
	  printf '%-15s %-10s %s\n' "$$c" "$$st" "$$hl"; \
	  if [ "$$st" != running ] || [ "$$hl" != healthy ]; then fail=1; fi; \
	done; \
	live="$$(docker exec namenode hdfs dfsadmin -report -live 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p' || true)"; \
	echo "HDFS live DataNodes : $${live:-?} (expected 3)"; \
	[ "$${live:-0}" -ge 3 ] || fail=1; \
	sm="$$(docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null || echo 'Safe mode is ?')"; \
	echo "HDFS safe mode      : $${sm##* } (expected OFF)"; \
	[ "$${sm##* }" = OFF ] || fail=1; \
	sw="$$(docker exec spark-master python3 -c 'import json, urllib.request as u; d = json.load(u.urlopen("http://localhost:8080/json/", timeout=5)); a = sorted(w["host"] for w in d["workers"] if w["state"] == "ALIVE"); print(len(a), *a)' 2>/dev/null || echo 0)"; \
	echo "Spark ALIVE workers : $$sw (expected $$n_exp)"; \
	[ "$${sw%% *}" -eq "$$n_exp" ] || fail=1; \
	if [ $$fail -ne 0 ]; then echo "HEALTH: FAIL"; exit 1; fi; \
	echo "HEALTH: OK"

topics:
	docker exec kafka bash /opt/stocklake/conf/kafka/create_topics.sh

# Dedicated throwaway topic smoke_test, so the project topics stay clean.
smoke-kafka: topics
	@id="smoke-$$(date +%s)-$$RANDOM"; \
	echo "producing $$id to smoke_test"; \
	docker exec -e SMOKE_ID="$$id" kafka bash -c '\
	  set -eu; export KAFKA_HEAP_OPTS=-Xmx256m; B=localhost:9092; K=/opt/kafka/bin; \
	  $$K/kafka-topics.sh --bootstrap-server $$B --create --if-not-exists \
	    --topic smoke_test --partitions 1 --replication-factor 1 >/dev/null; \
	  printf "%s\n" "$$SMOKE_ID" | $$K/kafka-console-producer.sh --bootstrap-server $$B --topic smoke_test; \
	  out="$$($$K/kafka-console-consumer.sh --bootstrap-server $$B --topic smoke_test \
	    --from-beginning --timeout-ms 10000 2>/dev/null || true)"; \
	  if grep -qxF "$$SMOKE_ID" <<<"$$out"; then echo "OK: received $$SMOKE_ID"; \
	  else echo "FAIL: $$SMOKE_ID not received"; exit 1; fi'

# Python check run in spark-driver after SparkPi: executors on every ALIVE worker, and
# `import stocklake` works on the executors (mounted ./src). No dollar signs allowed here.
define SMOKE_SPARK_PY
import json, sys, time, urllib.request
from pyspark.sql import SparkSession

def get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)

master = get("http://spark-master:8080/json/")
alive = sorted(w["host"] for w in master["workers"] if w["state"] == "ALIVE")
print("registered ALIVE workers:", len(alive), alive)
if not alive:
    sys.exit("FAIL: no ALIVE Spark worker")

spark = SparkSession.builder.appName("smoke-spark-executors").getOrCreate()
sc = spark.sparkContext
api = sc.uiWebUrl + "/api/v1/applications/" + sc.applicationId + "/executors"
hosts = set()
for _ in range(60):
    hosts = {e["hostPort"].rsplit(":", 1)[0] for e in get(api) if e["id"] != "driver"}
    if len(hosts) >= len(alive):
        break
    time.sleep(2)

def probe(_):
    import socket
    import time
    import stocklake  # noqa: F401  proves executors see the package
    time.sleep(0.3)
    return socket.gethostname()

n = len(alive) * 8
ran_on = sorted(set(sc.parallelize(range(n), n).map(probe).collect()))
spark.stop()
print("executor hosts :", sorted(hosts))
print("tasks ran on   :", ran_on)
missing = sorted(set(alive) - set(ran_on))
if missing:
    sys.exit("FAIL: no task ran on " + ", ".join(missing))
print("OK: executors used on all", len(alive), "registered workers")
endef
export SMOKE_SPARK_PY

smoke-spark:
	@echo "== SparkPi on spark://spark-master:7077"
	@out="$$(docker exec spark-driver spark-submit --class org.apache.spark.examples.SparkPi \
	  /opt/spark/examples/jars/spark-examples.jar 100 2>&1)" || { echo "$$out" | tail -40; exit 1; }; \
	grep -E 'Pi is roughly' <<<"$$out" || { echo "$$out" | tail -40; echo "FAIL: SparkPi gave no result"; exit 1; }
	@echo "== executor placement"
	docker exec spark-driver python3 -c "$$SMOKE_SPARK_PY"

# Runs inside the Spark image (Java + pyspark), repo mounted read-only at /work.
# pytest is installed per run into /tmp (throwaway), not baked into the image.
# SPARK_CONF_DIR points to an empty dir so spark-defaults.conf (cluster master) is ignored.
test:
	docker run --rm --entrypoint bash \
	  -v "$(CURDIR)":/work:ro -w /work \
	  -e HOME=/tmp -e PYTHONPATH=/work/src \
	  -e SPARK_CONF_DIR=/tmp/no-spark-conf -e SPARK_MASTER='local[2]' \
	  $(SPARK_IMAGE) -c 'pip install --quiet --no-warn-script-location --target /tmp/pytest-deps pytest==9.2.0 \
	    && PYTHONPATH=/tmp/pytest-deps:$$PYTHONPATH python3 -m pytest -p no:cacheprovider tests/unit $(PYTEST_ARGS)'

lint:
	@if command -v ruff >/dev/null 2>&1; then ruff check .; else $(PYTHON) -m ruff check .; fi

# Runs on the host (Mac .venv or PC python3): stocklake.contracts is stdlib-only, so it needs
# only ./src. Calls to_markdown() directly instead of `-m stocklake.contracts.convert`, which runpy
# would execute a second time (contracts/__init__ already imports it -> RuntimeWarning).
docs:
	PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -c \
	  'from stocklake.contracts.convert import to_markdown; print(to_markdown())' \
	  > docs/data_dictionary.md.tmp
	mv docs/data_dictionary.md.tmp docs/data_dictionary.md

stats:
	@ids="$$(docker ps -q --filter label=com.docker.compose.project=$(PROJECT))"; \
	if [ -z "$$ids" ]; then echo "no running $(PROJECT) containers"; exit 0; fi; \
	docker stats --no-stream $$ids

# ---------------------------------------------------------------- Lát 1: sample pipeline
# Jobs print one JSON line on stdout; logs go to stderr. Spark jobs: the long spark-submit
# output is shown only on failure, otherwise just the job's JSON line.
define spark_job
out="$$($(SPARK_SUBMIT) $(1) 2>&1)" || { echo "$$out" | tail -60; echo "FAIL: spark-submit $(1)"; exit 1; }; \
grep -E '^\{"' <<<"$$out"
endef

ingest-daily:
	@echo "== daily OHLCV -> raw (collector)"
	@docker exec collector python $(JOBS_DIR)/ingest/daily_ohlcv.py $(INGEST_ARGS)

# Job exit codes: 0 ok, 1 a symbol failed, 2 blocked by the source (429/403), 3 trading hours.
ingest-intraday-history:
	@echo "== 1-minute history -> raw/intraday_history (collector, VCI 1 req/s)"
	@docker exec collector python $(JOBS_DIR)/ingest/intraday_history.py \
	  $(if $(SYMBOLS),--symbols $(SYMBOLS),--group $(HIST_GROUP)) $(HIST_ARGS)

# Reads and rewrites raw/intraday_history over WebHDFS (no source calls). Each file is written to a
# temp file and verified before it replaces the original; exit 1 if any file fails the check.
regroup-intraday-history:
	@echo "== regroup raw/intraday_history: one row group per VN month (collector, no VCI calls)"
	@docker exec collector python $(JOBS_DIR)/ingest/regroup_intraday_history.py $(REGROUP_ARGS)

# Reads raw/intraday_history over WebHDFS (no source calls). Banner on stderr: e2e-sample reads
# the replay JSON line from stdout.
replay:
	@echo "== replay $(REPLAY_DAYS) days of $(REPLAY_GROUP) at $(REPLAY_RATE) msg/s -> Kafka intraday_prices (collector)" >&2
	@docker exec collector python $(JOBS_DIR)/streaming/replay.py \
	  --group $(REPLAY_GROUP) --days $(REPLAY_DAYS) --rate $(REPLAY_RATE) $(REPLAY_ARGS)

stream-intraday-once:
	@echo "== Kafka intraday_prices -> raw (availableNow, checkpoint on HDFS)"
	@$(call spark_job,$(JOBS_DIR)/streaming/intraday_to_raw.py --available-now $(STREAM_ARGS))

verify-replay:
	@if [ -z "$(RUN_ID)" ] || [ -z "$(EXPECTED)" ]; then \
	  echo "usage: make verify-replay RUN_ID=<id> EXPECTED=<sent>"; exit 2; fi
	@echo "== verify replay $(RUN_ID): $(EXPECTED) sent == distinct records in raw"
	@$(call spark_job,$(JOBS_DIR)/streaming/verify_replay.py --run-id $(RUN_ID) --expected $(EXPECTED))

etl-clean:
	@echo "== daily raw -> clean"
	@$(call spark_job,$(JOBS_DIR)/etl/daily_raw_to_clean.py)
	@echo "== intraday raw -> clean $(CLEAN_ARGS)"
	@$(call spark_job,$(JOBS_DIR)/etl/intraday_raw_to_clean.py $(CLEAN_ARGS))

# The job exits 0 whenever it ran; each check's pass/fail is inside its JSON line.
dq-intraday:
	@echo "== DQ intraday_1m: day coverage vs VNINDEX calendar, minute vs daily volume"
	@$(call spark_job,$(JOBS_DIR)/dq/intraday_dq.py $(DQ_ARGS))

# Order: daily ingest, minute history, replay (run_id + sent from its JSON line), stream once,
# verify, clean, DQ, then the Parquet listing. Any failing step stops the recipe non-zero.
# The history step passes --allow-trading-hours only because the sample is small (10 symbols,
# a few requests each for the current year); the VN30 wave must never use it.
# DQ runs with --no-write: e2e prints the results without appending to curated/dq_report;
# pass/fail against the approved thresholds is in the job's JSON line, not its exit code.
e2e-sample:
	@$(MAKE) --no-print-directory ingest-daily
	@$(MAKE) --no-print-directory ingest-intraday-history HIST_GROUP=sample SYMBOLS= \
	  HIST_ARGS=--allow-trading-hours
	@line="$$($(MAKE) --no-print-directory replay REPLAY_GROUP=sample | tail -1)"; \
	echo "$$line"; \
	run_id="$$(sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' <<<"$$line")"; \
	sent="$$(sed -n 's/.*"sent":\([0-9]*\).*/\1/p' <<<"$$line")"; \
	if [ -z "$$run_id" ] || [ -z "$$sent" ]; then echo "FAIL: no run_id/sent in replay output"; exit 1; fi; \
	$(MAKE) --no-print-directory stream-intraday-once; \
	$(MAKE) --no-print-directory verify-replay RUN_ID="$$run_id" EXPECTED="$$sent"
	@$(MAKE) --no-print-directory etl-clean
	@$(MAKE) --no-print-directory dq-intraday DQ_ARGS=--no-write
	@echo "== Parquet files per table in /data/raw and /data/clean"
	@docker exec namenode hdfs dfs -ls -R /data/raw /data/clean | grep -E '\.parquet$$' \
	  | awk '{n = split($$8, p, "/"); print "/" p[2] "/" p[3] "/" p[4]}' | sort | uniq -c
	@echo "== per table: DIR_COUNT FILE_COUNT CONTENT_SIZE PATH"
	@docker exec namenode hdfs dfs -count /data/raw/daily_ohlcv /data/raw/intraday_history \
	  /data/raw/intraday_prices /data/clean/daily_ohlcv /data/clean/intraday_1m
	@echo "E2E-SAMPLE: OK"

clean-all:
	@if [ "$(CONFIRM)" != "yes" ]; then \
	  echo "clean-all DELETES every $(PROJECT) volume (HDFS, Kafka, collector state)."; \
	  echo "Re-run as: make clean-all CONFIRM=yes"; exit 1; fi
	$(COMPOSE) --profile w3 down -v --remove-orphans
