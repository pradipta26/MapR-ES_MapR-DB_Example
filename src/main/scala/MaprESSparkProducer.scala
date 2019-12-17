package streams

import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import com.typesafe.config.ConfigFactory


class MaprESSparkProducer(spark : SparkSession){
  //Populate environment specific variables
  println(s"\n\n Producer code is executing........\n\n")
  private val config = ConfigFactory.load("application.conf")
  private val bootstrapServers = config.getString("maprfs.bootstrap.servers")
  private val maprFSTopic = config.getString("1mbda.1cat.itemDetails.topic")
  private val fileCheckpoint = config.getString("1mbda.1cat.itemDetails.fileInput.checkpointLocation")
  private val streamCheckpoint = config.getString("1mbda.1cat.itemDetails.streamOutput.checkpointLocation")
  private val inputCSVFileFolder = config.getString("1mbda.1cat.itemDetails.input.csv.fileLocation")
  println(s"\n\n bootsStarp = ${bootstrapServers} \n maprFSTopic = ${maprFSTopic} \n inputCSVFileFolder = ${inputCSVFileFolder}\n " +
    s"fileCheckpoint = ${fileCheckpoint} \n streamCheckpoint = ${streamCheckpoint}\n\n")

  def processFileData() : Unit = { // Set the default stream and topic to publish to.

    //Process streaming data from file, manipulate the data and send it to mapR topic
    processItemFiles()

    println("All done.")
  }

  private def processItemFiles ()  : Unit = { //: Dataset[Row] = {

    println(s"\n\n processItemFiles code is executing........\n\n")
    val itemCSVInputSchema = getInputCSVSchema
    val csvInputStreamDF = spark.readStream
      .format("csv")
      .option("sep", ",")
      .schema(itemCSVInputSchema)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("inferSchema", "false")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      //.option("checkpointLocation", fileCheckpoint)
      .load(inputCSVFileFolder)
      //.dropDuplicates("_id")
      .drop("Stock", "Upsells", "CrossSells", "ExternalURL", "ButtonText", "Position")
    println(s"\n\n csvInputStreamDF is Loaded........\n\n")

    val tempInputStreamDF = csvInputStreamDF
      .withColumn("attribute1", struct ("attribute1Name", "attribute1Values", "attribute1visible", "attribute1global"))
      .withColumn("attribute2", struct("attribute2Name", "attribute2Values", "attribute2visible", "attribute2global"))
      .drop("attribute1Name", "attribute1Values", "attribute1visible", "attribute1global", "attribute2Name", "attribute2Values", "attribute2visible", "attribute2global")
    println(s"\n\n tempInputStreamDF is Loaded........\n\n")

    import spark.implicits._

    val nestedItemDF = tempInputStreamDF
      .withColumn("dimension", struct($"weightInLbs", $"lengthInInch", $"widthInInch", $"heightInInch"))
      .withColumn( "options", struct($"attribute1", $"attribute2"))
      .drop("weightInLbs", "lengthInInch", "widthInInch", "heightInInch", "attribute1", "attribute2")
    println(s"\n\n nestedItemDF is Loaded........\n\n")

    println(s"writeToMaprFS code is executing........\n\n")
    import org.apache.spark.sql.streaming.Trigger
    val query = nestedItemDF.select($"_id".alias("key"), to_json(struct("*")).alias("value"))
      .writeStream.format("kafka")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", streamCheckpoint)
      .option("kafka.bootstrap.servers",bootstrapServers)
      .option("topic", """/apps/1MBDA/1CAT/itemStream:itemDetail""")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def getInputCSVSchema(): StructType ={

    val itemCSVInputSchema = StructType (
      StructField("_id", StringType, false) ::
        StructField("type", StringType, false) ::
        StructField("SKU", StringType, true) ::
        StructField("name", StringType, true) ::
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
        StructField("weightInLbs", DoubleType, true) ::
        StructField("lengthInInch", DoubleType, true) ::
        StructField("widthInInch", DoubleType, true) ::
        StructField("heightInInch", DoubleType, true) ::
        StructField("isCustomerReviewsAllowed", IntegerType, true) ::
        StructField("PurchaseNote", StringType, true) ::
        StructField("SalePrice", DoubleType, true) ::
        StructField("RegularPrice", DoubleType, true) ::
        StructField("Categories", StringType, true) ::
        StructField("Tags", StringType, true) ::
        StructField("ShippingClass", StringType, true) ::
        StructField("Images", StringType, true) ::
        StructField("DownloadLimit", IntegerType, true) ::
        StructField("DownloadExpiryDays", StringType, true) ::
        StructField("Parent", StringType, true) ::
        StructField("GroupedProducts", StringType, true) ::
        StructField("Upsells", StringType, true) ::
        StructField("CrossSells", StringType, true) ::
        StructField("ExternalURL", StringType, true) ::
        StructField("ButtonText", StringType, true) ::
        StructField("Position", IntegerType, true) ::
        StructField("attribute1Name", StringType, true) ::
        StructField("attribute1Values", StringType, true) ::
        StructField("attribute1visible", IntegerType, true) ::
        StructField("attribute1global", IntegerType, true) ::
        StructField("attribute2Name", StringType, true) ::
        StructField("attribute2Values", StringType, true) ::
        StructField("attribute2visible", IntegerType, true) ::
        StructField("attribute2global", IntegerType, true)::
        StructField("wpcomIsMarkdown", IntegerType, true) ::
        StructField("download1Name", StringType, true) ::
        StructField("download1URL", StringType, true) ::
        StructField("download2Name", StringType, true) ::
        StructField("download2URL", StringType, true) :: Nil)
    itemCSVInputSchema
  }


}
