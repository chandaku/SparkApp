package com.main

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{aggregate, col, desc, split, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostPopularHeroDataSet extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MostPopularHeroDataSet")
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
    .sort(desc("connections"))
    .first()

  val heroNamePopular = heroName
    .filter($"id" === heroConnections(0) )
    .first()

  print(s"Most popular hero name is ${heroNamePopular.name}")



}
case class MarvelGraph(value: String)

case class MarvelName(id: Int, name:String)
