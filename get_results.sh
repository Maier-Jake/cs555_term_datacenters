#!/bin/bash
set -e

LOCAL_DATA_DIR="data"
HDFS_ROOT="/data"

hdfs dfs -get /data/output/avg_datacenter_effect_by_state ./local_results/