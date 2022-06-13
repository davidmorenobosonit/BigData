package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}
/**
 * @author ${user.name}
 */

object App {
  def main(args : Array[String]) {
    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    // Get the M&M data set filename
    val mnmFile = "mnm_dataset.csv" //args(0)
    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)
    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()
    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }
}