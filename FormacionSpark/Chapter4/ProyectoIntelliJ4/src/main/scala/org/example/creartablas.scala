package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author ${David Moreno}
 */

object creartablas {
  def main(args : Array[String]) {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("SQLqueries")
      .getOrCreate()

    /* Vamos a ver como utilizar queries de SQL en spark. Estas se llaman con SQL() */

    // Vamos a leer un dataframe a modo de ejemplo
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("departuredelays.csv")


    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.sql("CREATE TABLE tablaejemplo (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    spark.sql("""Create table unmanagedtablaejemplo(date string, delay int, distance int, origin int, destination string) using csv""")

    // Stop the SparkSession
    spark.stop()
  }
}