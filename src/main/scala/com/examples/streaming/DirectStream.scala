package com.examples.streaming


import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.dstream.InputDStream

object DirectStream extends App {

  // create Spark configuration
  val conf = new SparkConf()
    .setAppName("kafkaStream-Application")
    .setMaster("local[*]") // local mode


  // create Streaming Context
  val ssc = new StreamingContext(conf, Seconds(5))

  // create Spark context
  val sc = ssc.sparkContext

  // stop logs messages displaying on spark console
  sc.setLogLevel("ERROR")

  // create Spark session
  val sessionSpark: SparkSession = SparkSession.builder
    .config(conf)
    .getOrCreate()


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "_group_id_SparkDirectStream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topic-1") //, "topic-2")
  val consumerStrategy
  : ConsumerStrategy[String, String] =
    Subscribe[String, String](topics, kafkaParams)


  val directKafkaStream
  : InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      consumerStrategy
    )

  val lagsSchema: StructType = new StructType()
    .add("topic", StringType)
    .add("lag", LongType)
    .add("time", TimestampType)

  import sessionSpark.implicits._

  directKafkaStream.foreachRDD { rdd =>
    val lagsDF: DataFrame =
      rdd.map(e => List(e.value()))
        .toDF()
        .select(from_json(col("value").cast("string"), lagsSchema) as "jsonEvent")
        .select("jsonEvent.*")
        .orderBy(col("lag"))

    val meanLagsDF: DataFrame =
      lagsDF
        .groupBy(window('time, "4 seconds", "1 seconds")) //, 'lag)
        .agg(mean("lag") as "meanLag")
        .orderBy(col("meanLag"))
//        .distinct()
        .select("window.start", "window.end", "meanLag") //, "lag")


    lagsDF.printSchema()
    meanLagsDF.printSchema()
    lagsDF.show()
    meanLagsDF.show()


//    rdd.foreach { record: ConsumerRecord[String, String] =>
//      val value = Seq(record.value()).toDF //.rdd
//        .select(from_json(col("value").cast("string"), lagsSchema) as "jsonEvent")
//        .select("jsonEvent.*")
//        .select(window('time, "4 seconds", "2 seconds"))
//
//      value.show
//    }
  }

  //  directKafkaStream.foreachRDD { rdd =>
  //    // Get the offset ranges in the RDD
  //    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  //    for (o <- offsetRanges) {
  //      println(s"offsetRanges: ${o.toString()}")
  //      println(s"topic: ${o.topic} | partition: ${o.partition} | offsets: ${o.fromOffset} to ${o.untilOffset}")
  //    }
  //
  //  }


  ssc.start
  ssc.awaitTermination()
}
