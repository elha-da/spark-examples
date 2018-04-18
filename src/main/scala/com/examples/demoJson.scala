package com.examples.avroToparquet


import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
      .load("src/main//resources/episodes.avro")

    dfAvro.show()

//    dfAvro.filter("doctor > 5")
//      .write
//      .parquet("src/main/resources/episodes.parquet")
//      .avro("src/main/resources/episodes5.avro")

    val dfParquet = spark
      .read
      .parquet("src/main//resources/episodes.parquet")

    dfParquet.show()

//  val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
//  primitiveDS.map(_ + 1).collect()

    // *********************************** StringJson to DF *********************************** //
    import spark.implicits._

    val addressesSchema = new StructType()
      .add("city", StringType)
      .add("state", StringType)
      .add("zip", StringType)

    val schema = new StructType()
      .add("firstName", StringType)
      .add("lastName", StringType)
      .add("age", IntegerType)
      .add("email", StringType)
      .add("addresses", ArrayType(addressesSchema))

    schema.printTreeString

//    val schemaAsJson = schema.json
//    println(schemaAsJson)
//    println(schema.prettyJson)

    val rowJsonDF = Seq("""
        {
          "firstName" : "Jacek",
          "lastName" : "Laskowski",
          "age" : 32,
          "email" : "jacek@japila.pl",
          "addresses" : [
            {
              "city" : "Warsaw",
              "state" : "N/A",
              "zip" : "02-791"
            }
          ]
        }
      """).toDF("rawjson")

//    val dt = DataType.fromJson(schemaAsJson)
//    println(dt.sql)

    val people = rowJsonDF
      .select($"rawjson")
//      .select(from_json($"rawjson", schema) as "json")
      .select(from_json(col("rawjson"), schema) as "json")
      .select("json.*") // <-- flatten the struct field
      .withColumn("address", explode(col("addresses"))) // <-- explode the array field
//      .select("firstName", "lastName", "age", "email", "address.*") // <-- flatten the struct field
      .select("*", "address.*") // <-- flatten the struct field
      .drop("addresses") // <-- no longer needed
      .drop("address") // <-- no longer needed

    people.show

  }

}
