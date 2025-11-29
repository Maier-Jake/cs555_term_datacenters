#!/bin/bash

# Quick-Fast Exit immediately if a command exits with a non-zero status.
set -e

# Step 1: Clean and build the project
echo "Cleaning and packaging the Scala project..."
sbt clean package

# Step 2: Submit the Spark job
echo "Submitting the Spark job..."

spark-submit \
  --class DataCenterPrices.Main \
  --master yarn \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  target/scala-2.12/datacenterprices_2.12-0.1.0-SNAPSHOT.jar \
  hdfs:///data

echo "Spark job completed successfully."

