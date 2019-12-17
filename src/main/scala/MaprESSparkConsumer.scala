package streams

import org.apache.spark.sql.functions.{col, struct, from_json}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import com.typesafe.config.ConfigFactory


import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

class MaprESSparkConsumer (spark : SparkSession) {

  println(s"\n\n Consumer code is executing........\n\n")
  //Populate environment specific variables
  private val config = ConfigFactory.load("application.conf")
  private val bootstrapServers = config.getString("maprfs.bootstrap.servers")
  private val maprFSTopic = config.getString("1mbda.1cat.itemDetails.topic")
  private val streamCheckpoint = config.getString("1mbda.1cat.maprStream.output.checkpointLocation")
  private val tableName = config.getString("1mbda.1cat.itemDetails.output.itemTableName")
  println(s"\n\n bootsStarp = ${bootstrapServers} \n maprFSTopic = ${maprFSTopic} \n streamCheckpoint = ${streamCheckpoint}\n tableName = ${tableName}\n\n")

 def processItemStream ()  : Unit = {
   import spark.implicits._
   val kafkaInputSchema = getInputSchema
   val maprStreamDF = spark.readStream.format ("kafka")
     .option ("kafka.bootstrap.servers", "maprdemo:9092")
     .option ("subscribe", """/apps/1MBDA/1CAT/itemStream:itemDetail""")
     .option ("startingOffsets", "earliest")
     .option ("failOnDataLoss", false)
     .option ("maxOffsetsPerTrigger", 1000)
     .load ()
     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
     .where($"value".contains("_id"))

   val itemStreamDF = maprStreamDF.select($"key", from_json($"value", kafkaInputSchema).as("payload")).selectExpr("key" , "payload.*")
    println (itemStreamDF.schema)

   println("write stream")

   val streamingQuery = maprStreamDF
     .writeStream
     .format(MapRDBSourceConfig.Format)
     .option(MapRDBSourceConfig.TablePathOption, """/apps/1MBDA/1CAT/itemTable""")
     .option(MapRDBSourceConfig.IdFieldPathOption, "key")
     .option(MapRDBSourceConfig.CreateTableOption, false)
     .option(MapRDBSourceConfig.BulkModeOption, true)
     .option("checkpointLocation", """/user/1MBDA/1CAT/itemPOC/input/itemDetails/maprFS_checkpoint/streamOut_temp2""")
     .outputMode("append").start

   streamingQuery.awaitTermination()

  }
  private def getInputSchema : StructType = {
    StructType (
      StructField("_id",StringType,false) ::
        StructField("type",StringType,false) ::
        StructField("SKU",StringType,true) ::
        StructField("name",StringType,true) ::
        StructField("isPublished", IntegerType, true) ::
        StructField("IsFeatured", IntegerType, true) ::
        StructField("VisibilityInCatalog", StringType, true) ::
        StructField("ShortDescription", StringType, true) ::
        StructField("Description", StringType, true) ::
        StructField("DateSalePriceStarts", StringType, true) ::
        StructField("DateSalePriceEnds", StringType, true) ::
        StructField("TaxStatus", StringType, true) ::
        StructField("TaxClass", StringType, true) ::
        StructField("InStock", IntegerType, true) ::
        StructField("Stock", StringType, true) ::
        StructField("IsBackordersAllowed", IntegerType, true) ::
        StructField("IsSoldIndividually", IntegerType, true) ::
        StructField("isCustomerReviewsAllowed",IntegerType,true)::
        StructField("PurchaseNote",StringType,true)::
        StructField("SalePrice",DoubleType,true)::
        StructField("RegularPrice",DoubleType,true)::
        StructField("Categories",StringType,true)::
        StructField("Tags",StringType,true)::
        StructField("ShippingClass",StringType,true)::
        StructField("Images",StringType,true)::
        StructField("DownloadLimit",IntegerType,true)::
        StructField("DownloadExpiryDays",StringType,true)::
        StructField("Parent",StringType,true)::
        StructField("GroupedProducts",StringType,true)::
        StructField("Upsells",StringType,true)::
        StructField("CrossSells",StringType,true)::
        StructField("ExternalURL",StringType,true)::
        StructField("ButtonText",StringType,true)::
        StructField("Position",IntegerType,true)::
        StructField("wpcomIsMarkdown",IntegerType,true)::
        StructField("download1Name",StringType,true)::
        StructField("download1URL",StringType,true)::
        StructField("download2Name",StringType,true)::
        StructField("download2URL",StringType,true) ::
        StructField("dimension", StructType(
          StructField("weightInLbs",DoubleType,true)::
            StructField("lengthInInch",DoubleType,true)::
            StructField("widthInInch",DoubleType,true)::
            StructField("heightInInch",DoubleType,true) :: Nil),false) ::
        StructField("options", StructType(
          StructField("attribute1",StructType(
            StructField("attribute1Name",StringType,true) ::
              StructField("attribute1Values",StringType,true) ::
              StructField("attribute1visible",IntegerType,true) ::
              StructField("attribute1global",IntegerType,true) :: Nil),false) ::
            StructField("attribute2",StructType(
              StructField("attribute2Name",StringType,true) ::
                StructField("attribute2Values",StringType,true) ::
                StructField("attribute2visible",IntegerType,true) ::
                StructField("attribute2global",IntegerType,true) :: Nil),false) :: Nil),false) :: Nil )
  } //End of getInputSchema method
}

