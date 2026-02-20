#!/bin/bash

set -e

echo "=============================================="
echo "Spark Driver - Waiting for cluster..."
echo "=============================================="

sleep 30

echo "=============================================="
echo "Submitting Spark job to cluster..."
echo "=============================================="

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-driver \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --name InstagramKafkaConsumer \
  /app/consumer.py
