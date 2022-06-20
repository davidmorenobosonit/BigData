package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */

object schema {
  def main(args : Array[String]) {
    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("schemaAPP")
      .getOrCreate()

    // Vamos a leer el archivo json con la ruta comprimida y completa
    val jsonFile = "blogs.json"
    val jsonFile2 = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter3\\ProyectoIntelliJ3\\blogs.json"

    //Vamos a definir el schema que queremos
    val schema = "First STRING, Last STRING, URL STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"

    // dataframe
    val df = spark.read.schema(schema).json(jsonFile2)

    //show
    df.show(5,false)
    df.printSchema()

    // otra forma
    val df2 = spark.read.format("json").load(jsonFile2)
    df2.printSchema()
    // Stop the SparkSession
    spark.close()
  }
}