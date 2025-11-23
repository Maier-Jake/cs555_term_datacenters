#!/bin/bash

# Author: Changsoo Jung
# Date: 2024-01-17
# Description:
#   The machines_and_ports.txt file will contain the machine names & port numbers, 
#   and the machine names & port numbers will be used to update a student's 
#   configuration files for Hadoop and Spark.

# Determine the directory where the script is located
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Read the machine names and port numbers from the machines_and_ports.txt file.
assigned_mps="$script_dir/machines_and_ports.txt"
lines=()

while IFS= read -r line; do
    lines+=("$line")
done < "$assigned_mps"
port1=${lines[8]}



# Create the hadoopConf and sparkConf directories
hadoop_conf_dir=~/hadoopConf
spark_conf_dir=~/sparkConf
if [ -d "$hadoop_conf_dir" ]; then
    rm -r "$hadoop_conf_dir"
fi
if [ -d "$spark_conf_dir" ]; then
    rm -r "$spark_conf_dir"
fi
mkdir "$hadoop_conf_dir"
mkdir "$spark_conf_dir"

##################################################################################
# README.md for Hadoop & Spark
# Read the template file and write to the actual file
tmp_readme_path="$script_dir/templates/README.md"
readme_path="$script_dir/README.md"
# Replace placeholders in the file
sed "s/\*\*namenode\*\*/${lines[0]}/g" "$tmp_readme_path" > "$readme_path"
sed -i "s/\*\*secondarynamenode\*\*/${lines[1]}/g" "$readme_path"
sed -i "s/\*\*resourcemanager\*\*/${lines[2]}/g" "$readme_path"
sed -i "s/\*\*datanode1\*\*/${lines[3]}/g" "$readme_path"
sed -i "s/\*\*datanode2\*\*/${lines[4]}/g" "$readme_path"
sed -i "s/\*\*datanode3\*\*/${lines[5]}/g" "$readme_path"
sed -i "s/\*\*datanode4\*\*/${lines[6]}/g" "$readme_path"
sed -i "s/\*\*datanode5\*\*/${lines[7]}/g" "$readme_path"

##################################################################################
# Hadoop configuration

# masters
# Read the template file and write to the actual file
tmp_master_path="$script_dir/templates/masters"
master_path="$hadoop_conf_dir/masters"
# Replace placeholders in the file
sed "s/\*\*namenode\*\*/${lines[1]}/g" "$tmp_master_path" > "$master_path"

# workers for Hadoop
# Read the template file and write to the actual file
tmp_worker_path="$script_dir/templates/workers"
worker_path="$hadoop_conf_dir/workers"
# Replace placeholders in the file
sed "s/\*\*datanode1\*\*/${lines[3]}/g" "$tmp_worker_path" > "$worker_path"
sed -i "s/\*\*datanode2\*\*/${lines[4]}/g" "$worker_path"
sed -i "s/\*\*datanode3\*\*/${lines[5]}/g" "$worker_path"
sed -i "s/\*\*datanode4\*\*/${lines[6]}/g" "$worker_path"
sed -i "s/\*\*datanode5\*\*/${lines[7]}/g" "$worker_path"

# all_nodes
# Read the template file and write to the actual file
tmp_all_path="$script_dir/templates/all_nodes"
all_path="$hadoop_conf_dir/all_nodes"
# Replace placeholders in the file
sed "s/\*\*machine1\*\*/${lines[0]}/g" "$tmp_all_path" > "$all_path"
sed -i "s/\*\*machine2\*\*/${lines[1]}/g" "$all_path"
sed -i "s/\*\*machine3\*\*/${lines[2]}/g" "$all_path"
sed -i "s/\*\*machine4\*\*/${lines[3]}/g" "$all_path"
sed -i "s/\*\*machine5\*\*/${lines[4]}/g" "$all_path"
sed -i "s/\*\*machine6\*\*/${lines[5]}/g" "$all_path"
sed -i "s/\*\*machine7\*\*/${lines[6]}/g" "$all_path"
sed -i "s/\*\*machine8\*\*/${lines[7]}/g" "$all_path"

# core-site.xml
# Read the template file and write to the actual file
tmp_core_path="$script_dir/templates/core-site.txt"
core_path="$hadoop_conf_dir/core-site.xml"
# Replace placeholders in the file
# Read the original file, replace **namenode** with lines[1], and write to the new file
sed "s/\*\*namenode\*\*/${lines[0]}/g" "$tmp_core_path" > "$core_path"
# Now take the new file, replace **port1** with lines[10], and overwrite it
sed -i "s/\*\*port1\*\*/$port1/g" "$core_path"

# hdfs-site.xml
# Read the template file and write to the actual file
tmp_hdfs_path="$script_dir/templates/hdfs-site.txt"
hdfs_path="$hadoop_conf_dir/hdfs-site.xml"
# Replace placeholders in the file
sed "s/\*\*namenode\*\*/${lines[0]}/g" "$tmp_hdfs_path" > "$hdfs_path"
sed -i "s/\*\*secondary\*\*/${lines[1]}/g" "$hdfs_path"
sed -i "s/\*\*port2\*\*/$((port1 + 1))/g" "$hdfs_path"
sed -i "s/\*\*port3\*\*/$((port1 + 2))/g" "$hdfs_path"
sed -i "s/\*\*port4\*\*/$((port1 + 3))/g" "$hdfs_path"
sed -i "s/\*\*port5\*\*/$((port1 + 4))/g" "$hdfs_path"
sed -i "s/\*\*port6\*\*/$((port1 + 5))/g" "$hdfs_path"

# mapred-site.xml
# Read the template file and write to the actual file
tmp_mapred_path="$script_dir/templates/mapred-site.txt"
mapred_path="$hadoop_conf_dir/mapred-site.xml"
# Replace placeholders in the file
sed "s/\*\*port7\*\*/$((port1 + 6))/g" "$tmp_mapred_path" > "$mapred_path"

# yarn-site.xml
# Read the template file and write to the actual file
tmp_yarn_path="$script_dir/templates/yarn-site.txt"
yarn_path="$hadoop_conf_dir/yarn-site.xml"
# Replace placeholders in the file
sed "s/\*\*resourcemanager\*\*/${lines[2]}/g" "$tmp_yarn_path" > "$yarn_path"
sed -i "s/\*\*port8\*\*/$((port1 + 7))/g" "$yarn_path"
sed -i "s/\*\*port9\*\*/$((port1 + 8))/g" "$yarn_path"
sed -i "s/\*\*port10\*\*/$((port1 + 9))/g" "$yarn_path"
sed -i "s/\*\*port11\*\*/$((port1 + 10))/g" "$yarn_path"
sed -i "s/\*\*port12\*\*/$((port1 + 11))/g" "$yarn_path"
sed -i "s/\*\*port13\*\*/$((port1 + 12))/g" "$yarn_path"
sed -i "s/\*\*port14\*\*/$((port1 + 13))/g" "$yarn_path"
sed -i "s/\*\*port15\*\*/$((port1 + 14))/g" "$yarn_path"

# log4j.properties
# Read the template file and write to the actual file
tmp_log4j_path="$script_dir/templates/hadoop_log4j.txt"
log4j_path="$hadoop_conf_dir/log4j.properties"
# Copy the template file to the actual file
cp "$tmp_log4j_path" "$log4j_path"

# cleanup_all.sh
# Read the template file and write to the actual file
tmp_cleanup_path="$script_dir/templates/cleanup_all.txt"
cleanup_path="$hadoop_conf_dir/cleanup_all.sh"
# Copy the template file to the actual file
cp "$tmp_cleanup_path" "$cleanup_path"

# monitor.sh
# Read the template file and write to the actual file
tmp_monitor_path="$script_dir/templates/monitor.txt"
monitor_path="$hadoop_conf_dir/monitor.sh"
# Copy the template file to the actual file
cp "$tmp_monitor_path" "$monitor_path"

##################################################################################
# Spark configuration

# workers for Spark
# Read the template file and write to the actual file
tmp_worker_path="$script_dir/templates/workers"
worker_path="$spark_conf_dir/workers"
# Replace placeholders in the file
sed "s/\*\*datanode1\*\*/${lines[3]}/g" "$tmp_worker_path" > "$worker_path"
sed -i "s/\*\*datanode2\*\*/${lines[4]}/g" "$worker_path"
sed -i "s/\*\*datanode3\*\*/${lines[5]}/g" "$worker_path"
sed -i "s/\*\*datanode4\*\*/${lines[6]}/g" "$worker_path"
sed -i "s/\*\*datanode5\*\*/${lines[7]}/g" "$worker_path"

# spark-defaults.conf
# Read the template file and write to the actual file
tmp_sparkdefaults_path="$script_dir/templates/spark-defaults.txt"
sparkdefaults_path="$spark_conf_dir/spark-defaults.conf"
# Replace placeholders in the file
sed "s/\*\*namenode\*\*/${lines[0]}/g" "$tmp_sparkdefaults_path" > "$sparkdefaults_path"
sed -i "s/\*\*port16\*\*/$((port1 + 15))/g" "$sparkdefaults_path"

# spark-env.sh
# Read the template file and write to the actual file
tmp_sparkenv_path="$script_dir/templates/spark-env.txt"
sparkenv_path="$spark_conf_dir/spark-env.sh"
# Replace placeholders in the file
sed "s/\*\*namenode\*\*/${lines[0]}/g" "$tmp_sparkenv_path" > "$sparkenv_path"
sed -i "s/\*\*port16\*\*/$((port1 + 15))/g" "$sparkenv_path"
sed -i "s/\*\*port17\*\*/$((port1 + 16))/g" "$sparkenv_path"

# fairscheduler.xml
# Read the template file and write to the actual file
tmp_fairscheduler_path="$script_dir/templates/fairscheduler.txt"
fairscheduler_path="$spark_conf_dir/fairscheduler.xml"
# Copy the template file to the actual file
cp "$tmp_fairscheduler_path" "$fairscheduler_path"

# log4j.properties
# Read the template file and write to the actual file
tmp_log4j_path="$script_dir/templates/spark_log4j.txt"
log4j_path="$spark_conf_dir/log4j.properties"
# Copy the template file to the actual file
cp "$tmp_log4j_path" "$log4j_path"

# metrics.properties
# Read the template file and write to the actual file
tmp_metrics_path="$script_dir/templates/spark_metrics.txt"
metrics_path="$spark_conf_dir/metrics.properties"
# Copy the template file to the actual file
cp "$tmp_metrics_path" "$metrics_path"

# Create logs directories
logs_dir=~/sparkConf/logs
work_dir=~/sparkConf/work

mkdir "$logs_dir"
mkdir "$work_dir"


echo ""
echo "Your Hadoop & Spark configuration settings are done! Please read the README.md file in the current directory."

