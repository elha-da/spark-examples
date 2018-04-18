package com.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.streaming.OutputMode


object KafkaExample extends App {

  val sessionSpark: SparkSession = SparkSession.builder
    .appName("StructuredNetworkWordCount")
    .master("local")
//    .master("local[*]")
//    .config("spark.master", "local")
    .getOrCreate()

  import sessionSpark.implicits._

  //  val file = "/resources/kafka_connect_test.txt"

  // Create DataFrame representing the stream of input lines from connection to kafka
  val dfKafkaSource: DataFrame = sessionSpark.readStream
    .format("kafka")
    .option("subscribe", "topic-1")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .load

  val dataS: Dataset[(String, String)] = dfKafkaSource.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  // Register the DataFrame as a temporary view
//  dataS.createOrReplaceTempView("lags")
//  val lagsDF = sessionSpark.sql("SELECT * FROM lags")

  val lagsSchema: StructType = new StructType()
    .add("topic", StringType)
    .add("lag", LongType)
    .add("time", TimestampType)


  val dataJson: DataFrame = dfKafkaSource
//    .select(col("value").cast("string"))
    .select(from_json(col("value").cast("string"), lagsSchema) as "jsonEvent")
    .select("jsonEvent.*")
//    .select("topic", "lag", "time")

  val values: DataFrame = dataS
//    .select('value.cast("string"))
    .select(from_json('value, lagsSchema).alias("jsonEvent"))
    .select("jsonEvent.*")



  val query = values.writeStream
//  val query = dataJson.writeStream
//    .outputMode("append")
    .outputMode(OutputMode.Append())
    .format("console")
    .start()

//  val dataRDDvect = values
//    .rdd
//    .map{
//      row => Vectors.dense(row.getAs[Seq[Double]]("lag").toArray)
//    }
//
//  val summary: MultivariateStatisticalSummary = Statistics.colStats(dataRDDvect)
//  println(summary.mean) // a dense vector containing the mean value for each column
//  println(summary.variance) // column-wise variance
//  println(summary.numNonzeros)


  query.awaitTermination()

}
