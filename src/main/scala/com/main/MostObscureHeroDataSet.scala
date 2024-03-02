
package com.main

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{aggregate, col, desc, min, split, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostObscureHeroDataSet extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MostObscureHeroDataSet")
    .getOrCreate()

  import spark.implicits._

  val heroGraph = spark.read
    .option("sep", " ")
    .text("data/Marvel-graph.txt")
    .as[MarvelGraph]

  val schema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

  val heroName = spark.read
    .schema(schema)
    .option("sep", " ")
    .csv("data/Marvel-names.txt")
    .as[MarvelName]

  val heroConnections = heroGraph.withColumn("id", split(col("value"), " ")(0))
    .withColumn("connections", functions.size(split(col("value"), " "))-1)
    .groupBy("id" )
    .agg(sum("connections").alias("connections"))

  val minConnectionCount = heroConnections.agg(min("connections")).first().getLong(0)

  val allMinConnected = heroConnections.filter($"connections" === minConnectionCount)

  allMinConnected.join(heroName, "id").select("name").show()


}

