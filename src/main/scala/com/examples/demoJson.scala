package com.examples.avroToparquet


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.databricks.spark.avro._


object demoJson {

  def initSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("Spark SQL basic example")
      .setMaster("local[*]")
      .set("spark.executor.memory", "1g")

    SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //      .config("spark.some.config.option", "some-value")
      .config("spark.sql.shuffle.partitions", 1)
      .config(sparkConf)
      //      .config("spark.executor.memory", "2g")
      .getOrCreate()
  }



  def main(args: Array[String]) {
/*    val spark = initSparkSession()

    val dfJson = spark
      .read
      .json("./resources/peoples.json")
//      .json("./resources/employees.json")

    val dfCsv = spark
      .read
      .format("csv")
      .option("header", "true")
//      .option("inferSchema", "true")
      .option("delimiter", ";") // default ","
      .csv("./resources/peoples.csv")


    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Displays the content of the DataFrame to stdout
    dfJson.show(30)
    dfJson.printSchema()
    dfJson.select($"name", $"age"+1).show()


    dfCsv.show(30)
    dfCsv.printSchema()
    val dfCsvNewAge = dfCsv.filter($"age" > 20)
      .select($"name", ($"age"+1).as("newAge"))
      .groupBy("newAge")
      .count()

    val dfCsvStudent = dfCsv.filter($"job" === "student")
      .select($"name", ($"age"+1).as("newAge"))
      .groupBy("newAge")
      .count()

    dfCsvNewAge.show()
    dfCsvStudent.show()

    // Register the DataFrame as a SQL temporary view
    dfCsv.createOrReplaceTempView("peoplesCSV")
    dfCsvNewAge.createOrReplaceTempView("peoplesCSVNewAge")

    spark.sql("SELECT * FROM peoplesCSV").show()
*/
    val spark = SparkSession.builder().master("local").getOrCreate()
    val dfAvro = spark
      .read
      .format("com.databricks.spark.avro")
      .load("./resources/episodes.avro")

    dfAvro.show()

//    dfAvro.filter("doctor > 5")
//      .write
//      .parquet("./resources/episodes.parquet")
//      .avro("./resources/episodes5.avro")

    val dfParquet = spark
      .read
      .parquet("./resources/episodes.parquet")

    dfParquet.show()
  }

}
