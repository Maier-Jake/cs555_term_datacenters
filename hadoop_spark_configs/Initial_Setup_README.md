1. Download `hadoop_spark_configs.zip`
2. Add the below lines to your .bashrc. If you have any other "module" related lines in your .bashrc, remove them.
	source /etc/profile.d/modules.sh
	module purge
	module load courses/csx55
3. Unzip `hadoop_spark_configs.zip` in your home directory (i.e., `~`)
4. Edit `~/hadoop_spark_configs/machines_and_ports.txt`. Choose eight machines from the list provided and add them to the file. Add your given port number to the bottom of the file. For example, `~/hadoop_spark_configs/machines_and_ports.txt` may look like:
```
machine0
machine2
machine3
machine4
machine5
machine6
machine7
1023
```
where `machine0` is the hostname of a CSB machine, e.g., `denver`.
5. Run `~/hadoop_spark_configs/setup_all.sh`
6. Run `cat ~/hadoop_spark_configs/README.md` or open the file. The bottom of this file lists your machines and their roles.
7. ssh to your namenode and run `hdfs namenode -format`. Type `Y` if prompted. **IMPORTANT** You have to run all HDFS and Spark related commands from your namenode.
8. Run `start-dfs.sh` to start HDFS. Run `stop-dfs.sh` to turn off your HDFS cluster. This does nothing to the data stored in HDFS, but HDFS will be inaccessible. If you want to use YARN, you must run `start-yarn.sh` from your resource manager.
9. Run `start-master.sh` and then `start-workers` to start your Spark cluster. Run `stop-workers.sh` and `stop-master.sh to turn off your Spark cluster.