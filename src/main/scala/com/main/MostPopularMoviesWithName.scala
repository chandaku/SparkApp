package com.main

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object MostPopularMoviesWithName {

  case class Movie(userId:Int,movieId:Int, rating:Int, timestamp: Long )

  def main(args: Array[String]): Unit = {

    val loadMovies: Map[Int, String] = {
      implicit val codec :Codec = Codec("ISO-8859-1")
      Source.fromFile("data/ml-100k/u.item").getLines()
        .map(line=> line.split('|'))
       // .filter(x=> x(0) !=null && x(1) !=null)
        .map(x=> (x(0).toInt,x(1)))
        .toMap
    }
    // Create Spark session
    val spark = SparkSession.builder
      .appName("MostPopularMovies")
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

    val nameDict = spark.sparkContext.broadcast(loadMovies)


   // val dataSet = df.as[(Int, Int, Int, Long)]
    val dataSet = df.as[Movie]

    val movieName : Int => String = (movieId: Int) => {
      nameDict.value.getOrElse(movieId, "UNKNOWN")
    }

    val lookUpName = functions.udf(movieName)

    val movieCount = dataSet.groupBy("movieId").count()

    val movieWithName = movieCount.withColumn("movieName", lookUpName(col("movieId")))

   movieWithName.sort(desc("count")).show()
   //dataSet.printSchema()


  }
}
