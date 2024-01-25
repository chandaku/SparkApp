package com.main


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingEventProcessor {

  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder
      .appName("KafkaStreamReader")
      .master("local[*]")
      .getOrCreate()


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("subscribe", "topic1")
      .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("startingOffsets", "latest")
      .load()

   df.printSchema()
    /*val schema = new StructType()
      .add("id", IntegerType)
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("dob_year", IntegerType)
      .add("dob_month", IntegerType)
      .add("gender", StringType)
      .add("salary", IntegerType)


    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")*/

    df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
