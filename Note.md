Delete Existing Setup : /home/mapr/wellsfargo/poc/itemStream/refreshEnv.sh

Step 1: Log into Sandbox : ssh mapr@127.0.0.1 -p 2222 -> User/Pass : mapr/mapr
	
	MapR Control System: Enter https://localhost:8443 or https://127.0.0.1:8443
	Hue: http://localhost:8888 or http://127.0.0.1:8888

Step 2: Create a directory for the data : mkdir -p /mapr/1MBDA/1CAT/streaming/data/Item_POC
		default folder is demo.mapr.com. If we use other folder like 1MBDA like here, we need to enter this cluster info
		 in /opt/mapr/conf/mapr-clusters.conf , like the second entry below: 
			demo.mapr.com secure=false maprdemo:7222
			1MBDA secure=false maprdemo:7222

Step 3: Create HDFS Directory under MFS for MapR Stream/Topic and Checkpoint:
	hadoop fs -mkdir -p /apps/1MBDA/1CAT/
	hadoop fs -mkdir -p /user/1MBDA/1CAT/itemPOC/input/itemDetails/maprFS_checkpoint
	hadoop fs -mkdir -p /apps/1MBDA/1CAT/streaming/data/Item_POC/		//For Data

Step 4: In MapR command line interface to create a stream and a topic(single partition):
	maprcli stream create -path /apps/1MBDA/1CAT/itemStream -produceperm p -consumeperm p -topicperm p	 //--defaultpartitions 3 (default:1)
	maprcli stream topic create -path /apps/1MBDA/1CAT/itemStream -topic itemDetail	//-partitions 3 (default: attribute defaultpartitions of the stream)
	** Delete a topic: maprcli stream topic delete -path /apps/1MBDA/1CAT/itemStream -topic itemDetail 

Step 5: verify topic: maprcli stream topic info -path /apps/1MBDA/1CAT/itemStream -topic itemDetailc
Step 7: Create /opt/mapr/conf/env_override.sh
		1. nano /opt/mapr/conf/env_override.sh
		2. Add
			export SPARK_HOME=//opt/mapr/spark/spark-2.2.1
			export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.7.0
			export HIVE_HOME=/opt/mapr/hive/hive-2.1
			export KAFKA_HOME=/opt/mapr/kafka/kafka-1.0.1
			export PATH=/opt/mapr/hive/hive-2.1/bin:/opt/mapr/kafka/kafka-1.0.1/bin:/opt/mapr/spark/spark-2.2.1/bin/:$PATH
		3. Update environemnt change: source /opt/mapr/conf/env_override.sh

Step 8: Spark Debug :
		export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
		then debug or run :
		Execute Consumer -> spark-submit --class streams.Process1MBDAStream --master local[*] /home/mapr/wellsfargo/poc/itemStream/MapR-ES_SparkStreaming-assembly-0.1.jar con
		Execute Producer -> spark-submit --class streams.Process1MBDAStream --master local[*] /home/mapr/wellsfargo/poc/itemStream/MapR-ES_SparkStreaming-assembly-0.1.jar pro

Step 9: Move file from local to HDFS automatically:
	hadoop fs -copyFromLocal /mapr/1MBDA/1CAT/streaming/data/Item_POC/itemDupTest.csv /apps/1MBDA/1CAT/streaming/data/Item_POC/	

Note:
To test Map-rES Topics:

cd to Kafka bin
	/opt/mapr/kafka/kafka-1.0.1/bin
To Send Msg
	./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic /apps/1MBDA/1CAT/itemStream:itemDetail

To receive MSG
	./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic /apps/1MBDA/1CAT/itemStream:itemDetail

Insert into MapR DB
	1. $ mapr dbshell
	2. insert /apps/1MBDA/1CAT/itemTable --id movie0000002 --v '{"_id":"movie0000002" , "title":"Developers on the Edge", "studio":"Command Line Studios"}'
		//Document with id: "movie0000002" inserted.
	3. select all data : find /apps/1MBDA/1CAT/itemTable
			     find /apps/1MBDA/1CAT/itemTable --_id 45
Spark Save Batch Dataframe : def saveToMapRDB(tablename: String, createTable: Boolean, bulkInsert: Boolean, idFieldPath: String): Unit // idFieldPath example = "_id"

Clear Kafka Topic:
bin/kafka

df.withColumn("id", when($"id".isNull, 0).otherwise(1)).show
# MapR-ES_MapR-DB_Example
