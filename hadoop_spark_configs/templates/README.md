# Hadoop & Spark Configuration Instructions

- This is a guide to setting up a Hadoop cluster, and it is based on the following:

1. Java 11
2. Hadoop 3.3.4
3. Spark 3.5.0

- Java, Hadoop, and Spark should be loaded if you update your `.bashrc` file and `source` it.

## Setting up the cluster

- Before setting up, prequisites are:

1. If you finished to apply the changes of your `.bashrc` file, move to the next step.
2. You need to unzip `hadoop_spark_configs.zip` to your home directory

- To set up the cluster, follow the steps below:
**IMPORTANT NOTE** You probably have already completed steps 1-4.

1. change directory to `~/hadoop_spark_configs/` as `cd ~/hadoop_spark_configs/`
2. Add 8 machines and your port number to `machines_and_ports.txt` as described in the instructions. 
3. run `setup_all_config.sh` by `./setup_all_config.sh`.
4. Right after running `setup_all_config.sh`, please check your machine names and their roles at the bottom this file.
5. Please `ssh` to your namenode.
6. Please run `hdfs namenode -format` and type `Y` to format your namenode.
7. Then, you can start `dfs` and `yarn` clusters now. Please check **Highly Recommended Notes** below.

**Troubleshooting Notes**

- NOTE: This will generate `hadoopConf` & `sparkConf` directories for you, and will create `.xml` files based on your machines and ports.
- NOTE: If you have some problems on your machines during this semester, try using different machines. If that doesn't work, please contact to TA. Here are the instructions after then:

1. Before re-running `setup_all_config.sh` script, please make sure that you stopped your dfs cluster and yarn cluster as `./stop-dfs.sh` in your namenode and `./stop-yarn.sh` in your resourcemanager.
2. Also, make sure that you stopped your spark cluster as `./stop-master.sh` and `./stop-workers.sh`.
3. Change directory to `hadoopConf` as `cd ~/hadoopConf`.
4. Make `monitor.sh` executable as `chmod +x monitor.sh`.
5. Run `hdfs namenode -format` and type `Y` to format your namenode.
6. Make `cleanup_all.sh` executable as `chmod +x cleanup_all.sh`.
7. Run `~/hadoopConf/cleanup_all.sh`.
8. Run `hdfs namenode -format` again.
9. Finally, rerun `setup_all_config.sh`.

**Highly Recommended Notes**

- NOTE: 1. Highly recommend that `ssh` to namenode to run dfs cluster as `./start-dfs.sh`.
- NOTE: 2. To use HDFS, please make sure that your dfs cluster is running in your namenode.
- NOTE: 2-1. You can check it by `jps` command, and `NameNode` is showing up. Then, you are ready to use HDFS.
- NOTE: 3. If you want to run Hadoop in local mode, please `ssh` to your secondarynamenode first.
- NOTE: 4. Highly recommend that `ssh` to resourcemanager to run yarn cluster as `./start-yarn.sh`.
- NOTE: 5. If you want to run Hadoop in yarn mode, please `ssh` to your resourcemanager first.
- NOTE: 6. If you want to check all machines' states, please run `~/hadoopConf/monitor.sh` anywhere or `monitor.sh` in `~/hadoopConf`.
- NOTE: 7. make sure that your Hadoop dfs cluster is running before starting your spark cluster.

**Your machines & each machine's role**

- namenode: **namenode**
- secondary namenode: **secondarynamenode**
- resource manager: **resourcemanager**
- datanode1: **datanode1**
- datanode2: **datanode2**
- datanode3: **datanode3**
- datanode4: **datanode4**
- datanode5: **datanode5**
