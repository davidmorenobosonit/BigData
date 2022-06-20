package org

import org.apache.spark.sql.SparkSession

object parquetFormat {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[1]").appName("colops").getOrCreate()

    // En primer liugar vamos a cargar los datos de un csv

    val datos = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("sf-fire-calls.csv")

    datos.show(5,false)

    // Ahora guardamos el archivo en formato parquet

    datos.write.format("parquet").mode("overwrite") //.option("compression","snnapy")
      .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter3\\ProyectoIntelliJ3\\datosParquet")

  }
}