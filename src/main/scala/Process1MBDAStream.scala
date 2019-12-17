package streams

import streams.util._

object Process1MBDAStream extends App{
  /*
  Sample command line -> pro/con filename topicName
   */
  require(args.length > 0, "\n\nPlease provide alteast pro/con option to select producer or consumer application " +
    "default fileName and topic will be used in case there is no args(1) and args(2)\n\n")

  val spark = Environment.getSession
  args(0) match {
    case "pro" => {
      val producer = new MaprESSparkProducer(spark)
      producer.processFileData
    }
    case "con" => {
      val consumer = new MaprESSparkConsumer(spark)
      consumer.processItemStream
    }
    case x => throw new IllegalArgumentException("""arg 0 is wrong. The value should be "pro" or "con"""")
  }
}
