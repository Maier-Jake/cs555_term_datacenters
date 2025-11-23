# CS555 Term Project
Fall 2025
Jake Maier and Eric Kearney
Impact of Data Centers on Residential Electricity Rates

## Build instructions
The project is built using sbt since this is a scala project.

There is a module available on CSU computers to enable sbt.
Usually only running `sbt clean package` is required after code changes.

This compiles and builds the required jar for spark. Running a spark job can be done by  
`spark-submit --class "DataCenterPrices.Main" --master **yarn-client / local** target/scala-2.12/datacenterprices_2.12-0.1.0-SNAPSHOT.jar **path-to-input-data-dir**`

## Hadoop/spark reference and instructions
before doing this, you should use public ssh keys to allow login to CSU machines without entering password.  
Create keys on your local machine (e.g., your personal laptop) and copy them to the remote
Lab CS120 host (make sure you run these commands on your personal machine):  
`ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa`  
`ssh-copy-id <username>@<hostname>`  
1. add the following lines to your ~/.bashrc  
`source /etc/profile.d/modules.sh
`  
`module purge`  
`module load courses/cs455` 

2. source your changes (from terminal prompt)  
`richmond~$ source .bashrc`  

3. Copy hadoopConf to your home directory. hadoopConf contains files that dictate the machines and port numbers used in the cluster.
We may run into issues if we both use the file as is and attempt to use our clusters simultaneously. To avoid this, one of us should change
the machine names to different machines available at CSU. 

**ENSURE ALL LOCATIONS OF A MACHINE NAME ARE CHANGED. Some machine names are used in multiple files, and errors will occur if there is an incorrect mismatch**

4. Format the hadoop filesystem. (THIS IS RUN ONLY ON INITIAL SET-UP RE-RUNNING THIS WILL ERASE DATA ON hadoop fs) Run on primary namenode (machine listed in hadoopConf/core-site.xml)

`richmond~$ hdfs namenode -format`  
6. To start the cluster, sign in on the primary namenode (machine listed in hadoopConf/core-site.xml). and in another terminal window sign in to the resourceManager machine (found in hadoopConf/yarn-site.xml). Execute the following in order.  
ON primary namenode `richmond~$ start-dfs.sh`  
ON resourceManager `salt-lake-city~$ start-yarn.sh`  
ON primary namenode `richmond~$ start-master.sh`  
ON primary namenode `richmond~$ start-workers.sh`  

7. When done using cluster, ensure that you free the ports used by running the following commands in order:  
ON primary namenode `richmond~$ stop-master.sh`  
ON primary namenode `richmond~$ stop-workers.sh`  
ON resourceManager `salt-lake-city~$ stop-yarn.sh`  
ON primary namenode `richmond~$ stop-dfs.sh`  

Congratulations, hadoop and spark should now be available and running in distributed mode.
