package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level
// import org.apache.spark.ml.source.image

/**
 * @author ${David Moreno}
 */

object DataSources {
  // Evitamos warnings innecesarios
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  def main(args : Array[String]) {
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("datasources")
      .getOrCreate()

    println("Vamos a ver cómo importar algunos datos en DataFrame mediante distintos formatos de entradas")

    println("Formato .csv")

    val file = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\departuredelays.csv"
    val dfcsv = spark.read.format("csv")
      .option("mode","PERMISSIVE")
      .option("header","true")
      .option("inferSchema","true")
      .load(file)

    dfcsv.show(5,false)

    println("Formato .parquet")
    val file2 = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.parquet"
    val dfparquet = spark.read.format("parquet").load(file2)

    dfparquet.show(5,false)

    println("Formato .json")

    val file3 = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\json\\*"
    val dfjson = spark.read.format("json").load(file3)

    dfjson.show(5,false)

    println("Pasamos ahora a ver algun guardado en la carpeta ooutputs")

    val location = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\prueba.json"
    dfjson.write.format("json").mode("overwrite").save(location)

    /*
    ********************************************************************************************************************
    FORMATO PARQUET
    ********************************************************************************************************************
     */
    logg.warn("Pasamos a estudiar formato PARQUET")

    println("Vamos a leer el archivo en formato parquet y loguardamos en un DataFrame")

    val rutaparquet = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.parquet"
    val parquetDF = spark.read.format("parquet").load(rutaparquet)

    println("mostramos por pantalla sus primeras lineas")
    parquetDF.show(3,false)

    println("Vamos a crear una tabla SQL unmanaged con los datos del parquet")

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        |USING parquet
        |OPTIONS (
        |path 'C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.parquet'
        |)
        |""".stripMargin)

    spark.sql("""show tables""").show(false)

    println("Una vez guardada, puedo hacer consultas sql")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show(false)

    println("AHORA VAMOS A HACER EL PROCESO INVERSO \n \nVamos a guardar el DataFrame como .parquet")

    parquetDF.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\parquetDF.parquet")

    println("Guardamos tambien como tabla SQL")

    //parquetDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")


    /*
    ********************************************************************************************************************
    FORMATO JSON
    ********************************************************************************************************************
     */
    logg.warn("Pasamos a estudiar formato JSON")

    println("Vamos a leer el archivo en formato json y lo guardamos en un DataFrame")

    val rutajson = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\json\\*"
    val jsonDF = spark.read.format("json").load(rutajson)

    println("mostramos por pantalla sus primeras lineas")
    jsonDF.show(10,false)

    println("Vamos a crear una tabla SQL unmanaged con los datos del json")

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_json
        |USING json
        |OPTIONS (
        |path 'C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\json\\*'
        |)
        |""".stripMargin)

    spark.sql("""show tables""").show(false)

    println("Una vez guardada, puedo hacer consultas sql")

    spark.sql("""SELECT * FROM us_delay_flights_tbl_json""").show(false)

    println("AHORA VAMOS A HACER EL PROCESO INVERSO \n \nVamos a guardar el DataFrame como .json")

    jsonDF.write.format("json")
      .mode("overwrite")
      .option("compression","gzip") // "snappy")
      .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\jsonDF")

    println("Guardamos tambien como tabla SQL")

    //jsonDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl_json")


    /*
    ********************************************************************************************************************
    FORMATO CSV
    ********************************************************************************************************************
     */
    logg.warn("Pasamos a estudiar formato CSV")

    println("Vamos a leer el archivo en formato csv y lo guardamos en un DataFrame")

    val rutacsv = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\csv\\*"

    println("Vamos a proporcionarle el schema")
    val schemacsv = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

    println("Ahora sí, introducimos el dataframe")
    val csvDF = spark.read.format("csv")
      .schema(schemacsv)
      .option("header","true").option("mode","FAILFAST").option("nullvalue","")
      .load(rutacsv)

    println("mostramos por pantalla sus primeras lineas")
    csvDF.show(10,false)


    println("Vamos a crear una tabla SQL unmanaged con los datos del csv")

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_csv
        |USING csv
        |OPTIONS (
        |path 'C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\csv\\*',
        |header 'true',
        |inferSchema 'true',
        |mode 'FAILFAST'
        |)
        |""".stripMargin)

    spark.sql("""show tables""").show(false)

    println("Una vez guardada, puedo hacer consultas sql")

    spark.sql("""SELECT * FROM us_delay_flights_tbl_csv""").show(false)

    println("AHORA VAMOS A HACER EL PROCESO INVERSO \n \nVamos a guardar el DataFrame como .csv")

    csvDF.write.format("csv")
      .mode("overwrite")
      .option("sep", ";")
      //.option("header","true") // "snappy")
      .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\csvDF")

    println("Guardamos tambien como tabla SQL")

    //csvDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl_csv")



    /*
    ********************************************************************************************************************
    FORMATO AVRO
    ********************************************************************************************************************
     */
    logg.warn("Pasamos a estudiar formato AVRO")

    println("Vamos a leer el archivo en formato AVRO y loguardamos en un DataFrame")

    val rutaavro = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\avro\\*"
    //val avroDF = spark.read.format("avro").load(rutaavro)

    //println("mostramos por pantalla sus primeras lineas")
    //parquetDF.show(3,false)

    println("Vamos a crear una tabla SQL unmanaged con los datos del avro")

    //spark.sql(
    // """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    //    |USING avro
    //    |OPTIONS (
    //    |path 'C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\avro\\*'
    //    |)
    //    |""".stripMargin)

    //spark.sql("""show tables""").show(false)

    println("Una vez guardada, puedo hacer consultas sql")

    //spark.sql("""SELECT * FROM us_delay_flights_tbl""").show(false)

    println("AHORA VAMOS A HACER EL PROCESO INVERSO \n \nVamos a guardar el DataFrame como .avro")

    //parquetDF.write.format("avro")
    //  .mode("overwrite")
    //  //.option("compression", "snappy")
    //  .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\avroDF")

    //println("Guardamos tambien como tabla SQL")

    //parquetDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")




    /*
    ********************************************************************************************************************
    FORMATO ORC
    ********************************************************************************************************************
     */
    logg.warn("Pasamos a estudiar formato ORC")

    println("Vamos a leer el archivo en formato orc y lo guardamos en un DataFrame")

    val rutaorc = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.orc\\*"
    val orcDF = spark.read.format("orc").load(rutaorc)

    println("mostramos por pantalla sus primeras lineas")
    orcDF.show(10,false)

    println("Vamos a crear una tabla SQL unmanaged con los datos del orc")

    spark.sql(
      """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_orc
        |USING orc
        |OPTIONS (
        |path 'C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.orc\\*'
        |)
        |""".stripMargin)

    spark.sql("""show tables""").show(false)

    println("Una vez guardada, puedo hacer consultas sql")

    spark.sql("""SELECT * FROM us_delay_flights_tbl_orc""").show(false)

    println("AHORA VAMOS A HACER EL PROCESO INVERSO \n \nVamos a guardar el DataFrame como .orc")

    orcDF.write.format("orc")
      .mode("overwrite")
      .option("compression","snappy")
      .save("C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\outputs\\orcDF")

    println("Guardamos tambien como tabla SQL")

    //orcDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl_orc")


    /*
    ********************************************************************************************************************
    IMÁGENES, <ARCHIVOS DE IMAGEN>
    ********************************************************************************************************************
     */
    logg.warn("Vamos a ver como manipular imagenes")

    println("Vamos a ver en primer lugar como importar imagenes")

    //val rutaorc = "C:\\Users\\antoniodavid.moreno\\Desktop\\BigData\\Spark\\Chapter4\\ProyectoIntelliJ4\\inputs\\2010-summary.orc\\*"
    //val orcDF = spark.read.format("orc").load(rutaorc)

    // Stop the SparkSession
    spark.stop()
  }
}