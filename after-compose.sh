#!/usr/bin/env bash
set -euo pipefail

echo "=== Running after-compose setup ==="

# Ensure script runs from repo root
cd "$(dirname "$0")"

wait_for_container() {
  local container="$1"
  echo "Waiting for container: $container"

  until docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; do
    sleep 3
  done

  echo "$container is running"
}

wait_for_postgres() {
  local container="$1"
  local user="$2"
  local db="$3"

  echo "Waiting for PostgreSQL: $container"

  until docker exec "$container" pg_isready -U "$user" -d "$db" >/dev/null 2>&1; do
    sleep 3
  done

  echo "$container PostgreSQL is ready"
}

wait_for_cassandra() {
  echo "Waiting for Cassandra..."

  until docker exec cassandra1 cqlsh --connect-timeout=10 --request-timeout=20 -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; do
    sleep 5
  done

  echo "Cassandra is ready"
}

run_cassandra_setup() {
  local max_attempts=6
  local attempt=1

  while [ "$attempt" -le "$max_attempts" ]; do
    if docker exec cassandra1 test -f /home/setup/setup.cql; then
      if docker exec cassandra1 cqlsh --connect-timeout=10 --request-timeout=60 -f /home/setup/setup.cql; then
        return 0
      fi
    elif [ -f cassandra/setup.cql ]; then
      if docker exec -i cassandra1 cqlsh --connect-timeout=10 --request-timeout=60 < cassandra/setup.cql; then
        return 0
      fi
    else
      echo "WARN: setup.cql not found in cassandra1 or ./cassandra. Skipping Cassandra setup."
      return 0
    fi

    echo "Cassandra schema setup failed; retrying in 10s ($attempt/$max_attempts)"
    attempt=$((attempt + 1))
    sleep 10
  done

  echo "ERROR: Cassandra schema setup failed after $max_attempts attempts"
  return 1
}

wait_for_tcp() {
  local container="$1"
  local host="$2"
  local port="$3"

  echo "Waiting for TCP: $host:$port from $container"

  until docker exec "$container" bash -c ":</dev/tcp/$host/$port" >/dev/null 2>&1; do
    sleep 3
  done

  echo "$host:$port is reachable from $container"
}

wait_for_container namenode
wait_for_container db-ecom
wait_for_container cassandra1
wait_for_container cassandra2
wait_for_container spark-master
wait_for_container metastore
wait_for_container airflow-webserver

echo "=== Hadoop setup ==="

if docker exec namenode test -f /home/setup/setup.sh; then
  docker exec namenode bash /home/setup/setup.sh
else
  echo "WARN: /home/setup/setup.sh not found in namenode. Skipping Hadoop setup."
fi

echo "=== Restoring e-commerce PostgreSQL database ==="

wait_for_postgres db-ecom postgres ecom

if docker exec db-ecom test -f /home/ecom_backup.sql; then
  if docker exec db-ecom pg_restore -l /home/ecom_backup.sql >/dev/null 2>&1; then
    docker exec db-ecom pg_restore \
      --clean \
      --if-exists \
      -U postgres \
      -d ecom \
      /home/ecom_backup.sql
  else
    docker exec db-ecom psql \
      -v ON_ERROR_STOP=1 \
      -U postgres \
      -d ecom \
      -f /home/ecom_backup.sql
  fi
else
  echo "WARN: /home/ecom_backup.sql not found in db-ecom. Skipping Postgres restore."
fi

echo "=== Cassandra setup ==="

wait_for_cassandra

run_cassandra_setup

echo "=== Spark Thrift Server setup ==="

wait_for_tcp spark-master metastore 9083

if docker exec spark-master bash -c "command -v pgrep >/dev/null 2>&1 && pgrep -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' >/dev/null 2>&1"; then
  echo "Spark Thrift Server is already running"
elif docker exec spark-master bash -c "command -v start-thriftserver.sh >/dev/null 2>&1"; then
  docker exec spark-master bash -c "start-thriftserver.sh \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.cores.max=2 \
    --conf spark.executor.cores=2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
    --hiveconf hive.metastore.uris=thrift://metastore:9083 \
    --hiveconf hive.server2.transport.mode=http \
    --hiveconf hive.server2.thrift.http.port=10001 \
    --hiveconf hive.server2.thrift.http.path=cliservice" || true
else
  echo "WARN: start-thriftserver.sh not found in spark-master. Skipping Spark Thrift Server."
fi

echo "=== Kafka topic setup ==="

for topic in login logout clickin clickout; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 >/dev/null
done

echo "=== Airflow setup ==="

airflow_uid="$(docker exec airflow-webserver id -u)"
docker exec -u 0 airflow-webserver bash -c "chown -R ${airflow_uid}:0 /opt/airflow/logs && chmod -R u+rwX,g+rwX /opt/airflow/logs"

if docker exec airflow-webserver test -f /opt/airflow/config/setup.sh; then
  docker exec airflow-webserver bash /opt/airflow/config/setup.sh
else
  echo "WARN: /opt/airflow/config/setup.sh not found in airflow-webserver. Skipping Airflow setup."
fi

echo "=== after-compose setup completed ==="
