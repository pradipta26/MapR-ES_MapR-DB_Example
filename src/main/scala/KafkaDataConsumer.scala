package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

class KafkaDataConsumer extends Serializable  {

  case class Uber(dt: String, lat: Double, lon: Double, base: String, rdt: String) extends Serializable
  // Parse string into Uber case class
  def parseUber(str: String): Uber = {
    val p = str.split(",")
    Uber(p(0), p(1).toDouble, p(2).toDouble, p(3), p(4))
  }
  case class UberC(dt: java.sql.Timestamp, lat: Double, lon: Double,
                   base: String, rdt: String,
                   cid: Integer, clat: Double, clon: Double) extends Serializable

  case class Center(cid: Integer, clat: Double, clon: Double) extends Serializable

  // Uber with unique Id and Cluster id and  cluster lat lon
  case class UberwId(_id: String, dt: java.sql.Timestamp, lat: Double, lon: Double,
                     base: String, cid: Integer, clat: Double, clon: Double) extends Serializable

  // enrich with unique id for Mapr-DB
  def createUberwId(uber: UberC): UberwId = {
    val id = uber.cid + "_" + uber.rdt
    UberwId(id, uber.dt, uber.lat, uber.lon, uber.base, uber.cid, uber.clat, uber.clon)
  }
  def main(args: Array[String]): Unit = {

    var topic: String = "/apps/uberstream:ubers"
    var tableName: String = "/apps/ubertable"
    var savedirectory: String = "/mapr/demo.mapr.com/data/ubermodel"

    if (args.length == 3) {
      topic = args(0)
      savedirectory = args(1)
      tableName = args(2)

    } else {
      System.out.println("Using hard coded parameters unless you specify topic model directory and table. <topic model table>   ")
    }

    val spark: SparkSession = SparkSession.builder()
      .appName("stream")
      .master("local[*]")
      .getOrCreate()

    /*val ccdf = spark.createDataset(ac)
    ccdf.show*/

    println("readStream ")

    import spark.implicits._

    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "maprdemo:9092")
      .option("subscribe", topic)
      .option("group.id", "testgroup")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    println(df1.schema)

    println("Enrich Transformm Stream")

    spark.udf.register(
      "deserialize",
      (message: String) => parseUber(message)
    )

    val df2 = df1.selectExpr("""deserialize(CAST(value as STRING)) AS message""")
      .select($"message".as[Uber])

   println("write stream")
/*
    val query3 = cdf.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, tableName)
      .option(MapRDBSourceConfig.IdFieldPathOption, "_id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option("checkpointLocation", "/tmp/uberdb")
      .option(MapRDBSourceConfig.BulkModeOption, true)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)

    query3.start().awaitTermination()
*/
  }
def test (spark : SparkSession) ={
  import com.mapr.db.spark.impl._
  import com.mapr.db.spark.streaming._
  import com.mapr.db.spark.sql._
  //import com.mapr.db.spark.streaming.MapRDBSourceConfig

  val csvInputDF = spark.read
    .format("csv")
    .option("sep", ",")
    .schema("itemCSVInputSchema")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("inferSchema", "false")
    .load("/mapr/1MBDA/1CAT/streaming/data/Item_POC/item_1.csv")

  csvInputDF.writeStream.format(MapRDBSourceConfig.Format)
    //.option(MapRDBSourceConfig.TablePathOption, """/apps/1MBDA/1CAT/itemTable""")
    .option(MapRDBSourceConfig.IdFieldPathOption, "_id")
    //.option(MapRDBSourceConfig.CreateTableOption, false)
    .option("checkpointLocation", """/user/1MBDA/1CAT/itemPOC/input/itemDetails/maprFS_checkpoint/streamOut""")
    .option("Operation", "Overwrite")
    .outputMode("append")
    //.saveToMapRDB("""/apps/1MBDA/1CAT/itemTable""") //, false)
}
}
