#!/usr/bin/env bash
# Start the NameNode; format it only on first start (empty name dir).
# Runs as the last step of the image's /opt/starter.sh (which first does
# `sudo chmod o+rwx /data` so the hadoop user can write the named volume).
set -euo pipefail

NAME_DIR="${NAMENODE_DIR:-/data/namenode}"

if [ ! -f "${NAME_DIR}/current/VERSION" ]; then
  if [ -d "${NAME_DIR}" ] && [ -n "$(ls -A "${NAME_DIR}" 2>/dev/null)" ]; then
    echo "ERROR: ${NAME_DIR} is not empty but has no current/VERSION; refusing to format." >&2
    echo "Inspect the nn_data volume (or run 'make clean-all CONFIRM=yes')." >&2
    exit 1
  fi
  echo "===> ${NAME_DIR} is empty: formatting NameNode (clusterid=${HDFS_CLUSTER_ID:-stocklake})"
  hdfs namenode -format -force -clusterid "${HDFS_CLUSTER_ID:-stocklake}"
else
  echo "===> ${NAME_DIR} already formatted, skipping format"
fi

exec hdfs namenode
