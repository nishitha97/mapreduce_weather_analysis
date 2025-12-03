#!/bin/bash
echo "Initializing HDFS directories..."

hdfs dfs -mkdir -p /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 777 /tmp
hdfs dfs -chmod -R 777 /user/hive/warehouse

echo "HDFS initialization complete."
