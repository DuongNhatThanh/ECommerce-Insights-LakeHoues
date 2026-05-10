#!/usr/bin/env bash
set -euo pipefail

hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold
hdfs dfs -chmod 777 /data /data/bronze /data/silver /data/gold
