#!/usr/bin/env bash
set -e

echo "Starting SSH..."
echo 'root:thanhdn' | chpasswd
sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config
service ssh start

echo "Checking HDFS namenode state..."

if [ ! -d "/hadoop/dfs/name/current" ]; then
  echo "Formatting HDFS namenode..."
  hdfs namenode -format -force
else
  echo "HDFS namenode already formatted."
fi

echo "Starting Hadoop DFS and YARN..."
start-dfs.sh
start-yarn.sh

echo "Hadoop services started."

tail -f /dev/null
