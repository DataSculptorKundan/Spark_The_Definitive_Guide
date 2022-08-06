package com.spark_the_definitive_guide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral

object Chapter_2_Introduction_to_Spark {
  def main(args: Array[String]): Unit = {
    println("Spark Job has been started...")

    val spark = SparkSession
      .builder
      .appName("Chapter_2_Introduction_to_Spark")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //Creating Range of number
    val my_Range = spark.range(1000).toDF("number")
    my_Range.show()

    //Find all even number to current dataframe --Transformation
    val EvenData = my_Range.where("number % 2 = 0")
    EvenData.show()

    //Count the all even number from divisBy2 -- Action
    println("The count of EvenData is : " + EvenData.count())

    EvenData.count().foreach(println)

    spark.stop()
    println("Spark Job has been Completed...")
  }
}
