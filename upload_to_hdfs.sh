#!/bin/bash
set -e

# Run this to upload the data to your running HDFS

LOCAL_DATA_DIR="data"
HDFS_ROOT="/data"

if [ ! -d "$LOCAL_DATA_DIR" ]; then
  echo "ERROR: Local folder '$LOCAL_DATA_DIR' does not exist."
  exit 1
fi

hdfs dfs -test -d $HDFS_ROOT || hdfs dfs -mkdir $HDFS_ROOT

# Remove old directories if they exist
hdfs dfs -test -d ${HDFS_ROOT}/cleaned_eia_years && hdfs dfs -rm -r ${HDFS_ROOT}/cleaned_eia_years
hdfs dfs -test -f ${HDFS_ROOT}/data_centers.csv && hdfs dfs -rm ${HDFS_ROOT}/data_centers.csv

hdfs dfs -mkdir ${HDFS_ROOT}/cleaned_eia_years

hdfs dfs -put ${LOCAL_DATA_DIR}/data_centers.csv ${HDFS_ROOT}/

hdfs dfs -put ${LOCAL_DATA_DIR}/cleaned_eia_years/*.csv ${HDFS_ROOT}/cleaned_eia_years/

echo "Showing final HDFS layout:"
hdfs dfs -ls -R ${HDFS_ROOT}
