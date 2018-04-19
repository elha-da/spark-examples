package com.examples.stats

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, LinearRegressionTrainingSummary}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.functions._


object MLlibrairie extends App {

  // 1. Create Spark configuration
  val conf = new SparkConf()
    .setAppName("SparkLR-Application")
    .setMaster("local[*]")  // local mode

  // 2. Create Spark context
  val sc = new SparkContext(conf)

  // 3. Create Spark session
  val sessionSpark: SparkSession = SparkSession.builder
//    .appName("StructuredNetworkWordCount")
//    .master("local")
    .config(conf)
    .getOrCreate()

  // For implicit conversion from RDD to DataFrame
  import sessionSpark.implicits._

///*

  /**
    * read File
    */
  // Load data from file
  val fileDir = "src/main/resources/spark-data/streaming/AFINN-111.txt"
//  val fileDir = "src/main/resources/peoples.txt"
  val dataFile: DataFrame = sessionSpark.read
//    .option("header", "false")
    .csv(fileDir)

  dataFile.show()

  /**
    * Linear Regression
    */
  // Load training data
  val training: DataFrame = sessionSpark.read
    .format("libsvm")
    .load("src/main/resources/spark-data/mllib/sample_linear_regression_data.txt")

  val lr: LinearRegression = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  // Fit the model
  val lrModel: LinearRegressionModel = lr.fit(training)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients}")
  println(s"Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary: LinearRegressionTrainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")

  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")


  /**
    * Summary statistics
    */
  val observations= sc.parallelize(
    Seq(
      Vectors.dense(1.0, 10.0, 100.0),
      Vectors.dense(2.0, 20.0, 200.0),
      Vectors.dense(3.0, 0.0, 300.0),
      Vectors.dense(4.0, 30.0, 0.0)
    )
  )

  observations.map(r => r.toArray).toDF().show()
  observations.map(r => r.toArray).toDF().describe().show()

  // Compute column summary statistics.
  val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
  println("Count: " + summary.count)
  println("Min: " + summary.min.toArray.mkString(" "))
  println("Max: " + summary.max.toArray.mkString(" "))
  println("L1-Norm: " + summary.normL1.toArray.mkString(" "))
  println("L2-Norm: " + summary.normL2.toArray.mkString(" "))
  println(s"num Non-zeros ${summary.numNonzeros}")  // number of nonzeros in each column
  println(s"mean ${summary.mean}")  // a dense vector containing the mean value for each column
  println(s"variance ${summary.variance}")  // column-wise variance


  /**
    * Correlations
    */
  val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5)) // a series
  // must have the same number of partitions and cardinality as seriesX
  val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

  // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
  // method is not specified, Pearson's method will be used by default.
  val correlationPearson: Double = Statistics.corr(seriesX, seriesY, "pearson")
  val correlationSpearman: Double = Statistics.corr(seriesX, seriesY, "spearman")
  println(s"correlation Pearson is: $correlationPearson")
  println(s"correlation Spearman is: $correlationSpearman")

  val data: RDD[Vector] = sc.parallelize(
    Seq(
      Vectors.dense(1.0, 10.0, 100.0),
      Vectors.dense(2.0, 20.0, 200.0),
      Vectors.dense(5.0, 33.0, 366.0))
  ) // note that each Vector is a row and not a column

  // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
  // If a method is not specified, Pearson's method will be used by default.
  val correlationMatrix: Matrix = Statistics.corr(data, "pearson")
  println(s"correlation Matrix : \n${correlationMatrix.toString}")
//*/

  /**
    * sliding data
    */
  val levels = Seq(
  // (year, month, dayOfMonth, hour, minute, second)
  ((2018, 12, 12, 12, 12, 12), 5),
  ((2018, 12, 12, 12, 12, 14), 9),

  ((2018, 12, 12, 13, 13, 13), 4),
  ((2018, 12, 12, 13, 13, 11), 3),
  ((2018, 12, 12, 13, 13, 15), 8),
  ((2018, 12, 12, 13, 13, 16), 4),
  ((2018, 12, 12, 13, 13, 17), 4),
  ((2018, 8,  13, 0, 0, 0), 10),
  ((2018, 5,  27, 0, 0, 0), 15))
    .map { case ((yy, mm, dd, h, m, s), a) => (LocalDateTime.of(yy, mm, dd, h, m, s), a) }
    .map { case (ts, a) => (Timestamp.valueOf(ts), a) }
    .toDF("time", "level")

  levels.show()

//  val q = levels
//    .select(
//      window($"time", "5 seconds"), // "1 seconds"),
//      $"level"
//    )
//    .select("window.start", "window.end", "level")
  val q = levels
    .withWatermark("time", "5 seconds")
    .groupBy(window('time, "5 seconds", "4 seconds"))
    .agg(sum("level") as "level_sum")
    .orderBy(col("window.start"))
    .select("window.start", "window.end", "level_sum")

  q.printSchema
  q.show()
}
