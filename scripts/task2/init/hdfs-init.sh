#!/bin/bash
set -euo pipefail

# Wait for HDFS to be available and create required directories
MAX_TRIES=60
TRY=0

while [ $TRY -lt $MAX_TRIES ]; do
  if /usr/local/hadoop/bin/hdfs dfs -ls / >/dev/null 2>&1; then
    echo "HDFS is available"
    break
  fi
  echo "Waiting for HDFS... ($TRY/$MAX_TRIES)"
  sleep 1
  TRY=$((TRY+1))
done

if [ $TRY -ge $MAX_TRIES ]; then
  echo "HDFS not available after $MAX_TRIES seconds. Exiting." >&2
  exit 1
fi

# Create necessary directories and set permissive permissions for Hive
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /tmp
/usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /tmp
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /user/hive/warehouse
/usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /user/hive/warehouse

echo "HDFS init complete"