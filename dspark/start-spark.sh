#!/usr/bin/env bash
set -e

echo "Starting SSH..."
echo 'root:thanhdn' | chpasswd
sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config
service ssh start

if [ -z "${SPARK_WORKLOAD:-}" ]; then
  echo "ERROR: SPARK_WORKLOAD is not set."
  echo "Expected: master or worker"
  exit 1
fi

if [ "$SPARK_WORKLOAD" = "master" ]; then
  echo "Starting Spark master..."
  /opt/spark/sbin/start-master.sh
elif [ "$SPARK_WORKLOAD" = "worker" ]; then
  if [ -z "${SPARK_MASTER:-}" ]; then
    echo "ERROR: SPARK_MASTER is not set for worker."
    echo "Example: spark://spark-master:7077"
    exit 1
  fi

  echo "Starting Spark worker..."
  echo "Connecting to Spark master: $SPARK_MASTER"
  /opt/spark/sbin/start-worker.sh "$SPARK_MASTER"
else
  echo "ERROR: Invalid SPARK_WORKLOAD: $SPARK_WORKLOAD"
  echo "Expected: master or worker"
  exit 1
fi

echo "Spark service started."

tail -f /dev/null
