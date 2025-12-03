#!/bin/bash
set -e

LOCAL_DATA_DIR="data"
HDFS_ROOT="/data"

# Remove local results directory if it exists
if [ -d "results" ]; then
  rm -rf results
fi

# Download results from HDFS
hdfs dfs -get /results .