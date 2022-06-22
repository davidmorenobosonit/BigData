package org.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */

object lecturaFotos {
  def main(args : Array[String]) {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("lecturaFotos")
      .getOrCreate()

    val imdf = spark.read.format("csv").load("C:/Users/antoniodavid.moreno/Desktop/BigData/Spark/Chapter 4/ProyectoIntelliJ4/departuredelays.csv")



    println("HOLA, voy a ver \n el esquema que se ha creado:\n ")
    imdf.printSchema()




    // Stop the SparkSession
    spark.stop()
  }
}