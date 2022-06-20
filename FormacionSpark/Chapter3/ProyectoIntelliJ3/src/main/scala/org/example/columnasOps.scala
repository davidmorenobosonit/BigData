package org

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object columnasOps {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("colops")
      .getOrCreate()

    // Vamos a introducir un schema y leer un archivo json para crear un DataFrame

    val sch = StructType( Array (
      StructField("Id",IntegerType,nullable = true),
      StructField("First", StringType,nullable = true),
      StructField("Last", StringType,nullable = true),
      StructField("Url", StringType,nullable = true),
      StructField("Published", StringType,false),
      StructField("Hits",IntegerType, nullable = true),
      StructField("Campaigns", ArrayType(StringType),nullable = true)
    ) )
    // cargamos los archivos
    val df = spark.read.schema(sch).json("blogs.json")
    //
    df.printSchema()
    df.show(5,false)

    //Veamos cosas
    df.sort(col("Id").desc).show()
  }
}