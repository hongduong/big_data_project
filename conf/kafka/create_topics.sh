#!/usr/bin/env bash
# Create the project topics (idempotent). Planning section 4.2.
# Usage (from the repo root on the PC):  make topics
#   = docker exec kafka bash /opt/stocklake/conf/kafka/create_topics.sh
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KBIN="${KAFKA_BIN:-/opt/kafka/bin}"
# The broker container exports KAFKA_HEAP_OPTS=-Xms1g -Xmx1g; CLI tools would inherit
# it and could push the 2GB container over its limit. Give the tools a small heap.
export KAFKA_HEAP_OPTS="${KAFKA_TOOLS_HEAP_OPTS:--Xmx256m}"

echo "===> Waiting for broker at ${BOOTSTRAP}"
for i in $(seq 1 30); do
  if "${KBIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: broker ${BOOTSTRAP} not reachable" >&2
    exit 1
  fi
  sleep 2
done

create_topic() {
  local name="$1" partitions="$2" rf="$3"
  "${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" --partitions "${partitions}" --replication-factor "${rf}"
}

#            topic            partitions  RF
create_topic intraday_prices  6           1   # key = symbol
create_topic market_news      1           1   # key = news_id

echo "===> Topics"
"${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
"${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe \
  --topic intraday_prices
"${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe \
  --topic market_news
