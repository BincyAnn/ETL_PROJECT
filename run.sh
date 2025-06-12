#!/bin/bash

# Path to the spark-submit executable
SPARK_SUBMIT=/home/bincy/.local/bin/spark-submit

# Path to the main PySpark script
MAIN_PY_SCRIPT=/home/bincy/PycharmProjects/kafkaapi/postgress.py

$SPARK_SUBMIT \
  --jars libs/spark-sql-kafka-0-10_2.12-3.0.1.jar \
  $MAIN_PY_SCRIPT


