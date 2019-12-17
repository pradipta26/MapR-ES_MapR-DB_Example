  package streams


  import scala.io.Source
  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord
  import java.util.Properties


  class KafkaDataProducer { // Declare a new producer

    @throws[Exception]
    def processFileData( absoluteFileName : String = "/mapr/demo.mapr.com/data/uber.csv",
                         topic : String = "/apps/uberstream:ubers") : Unit = { // Set the default stream and topic to publish to.

      val bufferedSource = Source.fromFile(absoluteFileName)
      val lines: List[String] = bufferedSource.getLines.toList.tail //Skip Header
      println("Sending to topic " + topic)
      val producer: KafkaProducer[String, String] = configureProducer()
      for (line <- lines) {
        if (line.length > 0) {
          val rec: ProducerRecord[String, String] = new ProducerRecord(topic, s"${line},${getReverseTimestamp}")
          producer.send(rec) // Send the record to the producer client library.
          System.out.println("Sent message: " + line)
          Thread.sleep(60L)
        }
      }
      bufferedSource.close()
      producer.close
      println("All done.")
     }

    /* Set the value for a configuration parameter.
         This configuration parameter specifies which class
         to use to serialize the value of each message.
     */
    private def configureProducer() : KafkaProducer[String,String] = {
      val props = new Properties
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer (props)
     }

    def getReverseTimestamp: Long = java.lang.Long.MAX_VALUE - java.lang.System.currentTimeMillis



}
