package com.main

import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object ReadCSV {

  case class Movie(userId:Int,movieId:Int, rating:Int, timestamp: Long )

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder
      .appName("KafkaStreamReader")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("userId", IntegerType, nullable = true)
      .add("movieId", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

   import spark.implicits._

   val df = spark.read
      .schema(schema)
      .option("sep", "\t")
      .csv("data/ml-100k/u.data")


   // val dataSet = df.as[(Int, Int, Int, Long)]
    val dataSet = df.as[Movie]
    dataSet.groupBy("movieId").count().orderBy(desc("count")).show()

   dataSet.printSchema()


  }
}
