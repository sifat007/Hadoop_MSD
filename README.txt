Login to namenode: salem
Run HDFS and Yarn
Logint to a client: albany
source set_client_env.sh
cd to project dir
Compile: ant
Remore previous output: $HADOOP_HOME/bin/hdfs dfs -rm -r /home/cs555/*-out
Run: $HADOOP_HOME/bin/hadoop jar dist/msd.jar cs555.hadoop.msd.MSDJob /home/cs555/sampleData /home/cs555/q1-out