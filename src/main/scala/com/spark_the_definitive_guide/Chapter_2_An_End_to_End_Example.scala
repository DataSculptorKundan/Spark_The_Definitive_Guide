package com.spark_the_definitive_guide

import org.apache.spark.sql.SparkSession

object Chapter_2_An_End_to_End_Example {
  def main(args: Array[String]): Unit = {
    println("Spark Job has been started...")

    val spark = SparkSession
      .builder
      .appName("Chapter_2_An_End_to_End_Example")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

// Reading a csv file into Dataframe
    val flightData2015 = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("data/flight-data/csv/2015-summary.csv")

    flightData2015.take(5).foreach(println) //Converting it to a local array
    flightData2015.sort("Count").explain()

//reduce the number of the output partition from shuffle
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    flightData2015.sort("count").take(5).foreach(println)
  }

}
