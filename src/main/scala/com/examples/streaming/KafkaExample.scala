package com.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaExample extends App {

    // 1. Create Spark configuration
  val conf = new SparkConf()
    .setAppName("kafkaExamples-Application")
    .setMaster("local[*]")  // local mode

  val ssc = new StreamingContext(conf, Seconds(5))

  ssc.sparkContext

  val sessionSpark: SparkSession = SparkSession.builder
//    .appName("kafkaExamples")
//    .master("local")
//    .master("local[*]")
//    .config("spark.master", "local")
    .config(conf)
    .getOrCreate()

  import sessionSpark.implicits._

  // Create DataFrame representing the stream of input lines from connection to kafka
  val dfKafkaSource: DataFrame = sessionSpark.readStream
    .format("kafka")
    .option("subscribe", "topic-1")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .load
//    .withWatermark("timestamp", "10 seconds")
//    .groupBy(window('time, "5 seconds", "4 seconds"))
//    .agg(mean("lag") as "level_sum")
//    .select(window(col("timestamp"), "5 seconds", "4 seconds"))


  val dataS: Dataset[(String, String)] = dfKafkaSource.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  // Register the DataFrame as a temporary view
//  dataS.createOrReplaceTempView("lags")
//  val lagsDF = sessionSpark.sql("SELECT * FROM lags")

  val lagsSchema: StructType = new StructType()
    .add("topic", StringType)
    .add("lag", ArrayType(LongType))
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
//    .groupBy(window('time, "5 seconds", "4 seconds"))
//    .agg(mean("lag") as "meanLag")
//    .select("window.start", "window.end", "meanLag")


  val query = values.writeStream
//  val query = dataJson.writeStream
//    .outputMode("append")
    .outputMode(OutputMode.Update())
    .format("console")
//    .start()



//  val dataRDDvect = values
//    .rdd
//    .map{
//      row => Vectors.dense(row.getAs[Seq[Double]]("lag").toArray)
//    }

//  val summ = values.describe("value")

//  val summary: MultivariateStatisticalSummary = Statistics.colStats(dataRDDvect)
//  println(summary.mean) // a dense vector containing the mean value for each column
//  println(summary.variance) // column-wise variance
//  println(summary.numNonzeros)



  query.start().awaitTermination()
//  query.awaitTermination()

}
