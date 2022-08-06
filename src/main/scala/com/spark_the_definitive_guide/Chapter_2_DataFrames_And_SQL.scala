package com.spark_the_definitive_guide

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{SparkSession, functions}

object Chapter_2_DataFrames_And_SQL {
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

// Make Dataframe into table or View to call simple method "createOrReplaceTempView"
    flightData2015.createOrReplaceTempView("flight_data_2015")

// Sql query against a Dataframe returns another DataFrame
    val sqlWay = spark.sql("""SELECT DEST_COUNTRY_NAME, count(1) from flight_data_2015 GROUP BY DEST_COUNTRY_NAME""")
    sqlWay.show()
    sqlWay.explain() //Physical compilation Plan in SQL way

    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
    dataFrameWay.show()
    dataFrameWay.explain() //Physical compilation Plan in DataFrame way

// Using transformation, filtering down the one row through SQL aggregation
    spark.sql("SELECT max(count) from flight_data_2015").take(1).foreach(println)

// Using transformation, filtering down the one row through DataFrame
    flightData2015.select(functions.max("count")).take(1).foreach(println)

// To find the top five destination Country through SQL aggregation
    val topFiveDest = spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as destination_total from flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY destination_total DESC LIMIT 5")
    topFiveDest.show()
    topFiveDest.explain()

// To find the top five destination Country through DataFrame way
    flightData2015.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

    flightData2015.explain()

  }

}
