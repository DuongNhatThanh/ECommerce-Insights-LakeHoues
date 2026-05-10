#!/usr/bin/env bash
set -euo pipefail

upsert_connection() {
    local conn_id="$1"
    shift

    airflow connections delete "$conn_id" >/dev/null 2>&1 || true
    airflow connections add "$conn_id" "$@"
}

upsert_connection spark_server \
    --conn-type ssh \
    --conn-host spark-master \
    --conn-login root \
    --conn-password thanhdn \
    --conn-port 22

upsert_connection hdfs_server \
    --conn-type ssh \
    --conn-host namenode \
    --conn-login root \
    --conn-password thanhdn \
    --conn-port 22

airflow connections delete sqoop_server >/dev/null 2>&1 || true
