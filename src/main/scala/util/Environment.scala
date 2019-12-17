package streams.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{struct, to_json, _}
import org.apache.spark.sql.{Dataset, Row}

object Environment {

  def getSession: SparkSession = {
    val spark = SparkSession.builder().appName("WF 1MBDA-1CAT POC")
      //.config("spark.sql.shuffle.partitions", 50)
      .config("spark.sql.warehouse.dir", """/home/hadoop/hive/warehouse""")
      .enableHiveSupport()
      .config("spark.sql.broadcastTimeout", 1200)
      //.config ("spark.executor.memory", "2g")
      .getOrCreate()
    return spark

  }
  def writeToKafka(bootstrapServers : String, host : Array[String],
                   port : Array[String], topics : Array[String], dataFrame : Dataset[Row], schema : StructType) = {
    val topic = topics.mkString(", ")
    dataFrame.select(col("PRODUCTID").cast("String").as("key"), to_json(struct("*")).alias("value"))
      .writeStream.format("kafka")
      .option("checkpointLocation", "/HDFS-compatible/ dir")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("topic", "topic1")
      .start()

  }
}