name := "SparkExample"

version := "0.0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val circeVersion = "0.9.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.databricks" %% "spark-avro" % "3.0.0",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.lz4" % "lz4-java" % "1.4.1"
)


