name := "MapR-ES_SparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"

lazy val sparkVersion = "2.3.0"

organization := "org.wellsfargo.1mbda"

fork := true

resolvers +=
  "mapr-releases" at "http://repository.mapr.com/maven/" //http is deprecated but https does not work first time

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "com.mapr.db" % "maprdb-spark" % "2.2.1-mapr-1803",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "0.10.1.1" % "provided",
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.1" % "provided",
  "com.typesafe" % "config" % "1.3.2"
)

scalacOptions += "-target:jvm-1.8"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
//resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


