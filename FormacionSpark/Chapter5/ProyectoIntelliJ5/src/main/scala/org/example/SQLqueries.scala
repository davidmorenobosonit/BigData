package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author ${David Moreno}
 */

object SQLqueries {
  // Evitamos warnings innecesarios
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  // Creamos la SparkSession
  def main(args : Array[String]) {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("SQLqueries")
      .getOrCreate()

    //.enableHiveSupport()
    //.config("spark.sql.catalogImplementation","hive")//.enableHiveSupport()

    /* Vamos a ver como utilizar queries de SQL en spark. Estas se llaman con SQL() */

    // Vamos a leer un dataframe a modo de ejemplo
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("inputs/departuredelays.csv")

    logg.warn("Leemos un DataSet en una Vista Temporal y hacemos algunas consultas sobre el")
    df.createOrReplaceTempView("tablaTempRandom")
    spark.sql(
      """select distance, origin, destination
        |from tablaTempRandom
        |where distance > 1000
        |order by distance desc
        |""".stripMargin).show(5)

    spark.sql("""SELECT date, delay, origin, destination
           FROM tablaTempRandom
           WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
           ORDER by delay DESC""").show(10)

    spark.sql("""SELECT delay, origin, destination, CASE
                                      WHEN delay > 360 THEN 'Very Long Delays'
                                      WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                                      WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                                      WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                                      WHEN delay = 0 THEN 'No Delays'
                                      ELSE 'Early'
                                      END AS Flight_Delays
                         FROM tablaTempRandom
                         ORDER BY origin, delay DESC""").show(10)

    logg.warn("Intentamos hacer eso pero con la API de dataframe")

    df.printSchema()

    df.select("distance","origin","destination")
      .where(col("distance")>1000)
      .sort(col("distance").desc).show(5)

    df.select("date", "delay", "origin", "destination")
      .where((col("delay")>120) and (col("origin")==="SFO") and (col("destination")==="ORD"))
      .sort(col("delay").desc).show(10)

    df.withColumn("Retraso",
        expr("case when delay>360 then 'Muchisimo retraso' " +
          "WHEN delay > 120 AND delay < 360 THEN 'Retraso grande'" +
          "WHEN delay > 60 AND delay < 120 THEN 'Retraso mediano'" +
          "WHEN delay > 0 and delay < 60 THEN 'Retrasillo'" +
          "WHEN delay = 0 THEN 'No Delays'  ELSE 'Early' END")).show(10)

    logg.warn("Vamos a intentar convertir la fecha en formato date")

    spark.sql(
      """select delay, distance, origin, destination,
        |cast((date+100000000) as char(30)) as fechalarga
        |from tablaTempRandom
        |""".stripMargin).createOrReplaceTempView("tabla2")
    spark.sql("""select * from tabla2""").show(5)

    println("ahora voy a dividir esa fechalarga en lo que me interesa")

    spark.sql(
      """select delay, distance, origin, destination,
        |substring(fechalarga,2,2) as month,
        |substring(fechalarga,4,2) as day,
        |substring(fechalarga,6,2) as hour,
        |substring(fechalarga,8,2) as min
        |from tabla2 """.stripMargin).createOrReplaceTempView("tabla3")

    spark.sql("""select * from tabla3""").show(5)

    println("Por ultimo, vot a crear una columna en formato fecha con el total del las columnas")

    spark.sql(
      """select delay, distance, origin, destination,
        |to_date(concat("20/",month,"/",day," ",hour,":",min,":00"),'yy/MM/dd hh:mm:ss') as fecha
        |from tabla3
        |""".stripMargin).show(3)

    logg.warn("Voy a ver las tablas que he creado de momento")
    spark.sql("show tables").show()

    spark.sql("show databases").show()
   // spark.sql("CREATE DATABASE learn_spark_db")
   // spark.sql("USE learn_spark_db")
    spark.sql("USE default")
    spark.sql("show databases").show()

    spark.sql("show tables").show()

    df.write.option("mode","overwrite").saveAsTable("managed_us_delay_fights_tbl")

    spark.sql("show tables").show()

    spark.sql(""" SELECT * FROM managed_us_delay_fights_tbl ORDER BY DATE desc""").show()

    println("Aqui daría el fallo")
    //spark.sql("CREATE TABLE managed_us_delay (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
               |USING csv OPTIONS (PATH 'C://Users//antoniodavid.moreno//Desktop//BigData//Spark//Chapter4//ProyectoIntelliJ4//inputs//departuredelays.csv')""".stripMargin)

    df.write.option("path","/tmp/data/us_flights_delay").saveAsTable("us_delay_flights_tbl2")

    spark.sql("show tables").show(false)

    spark.sql(""" SELECT * FROM us_delay_flights_tbl ORDER BY DATE desc""").show()

    logg.warn("Creamos vistas temporales a partir detablas SQL")

    spark.sql(
      """CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
        |SELECT date, delay, origin, destination
        |from us_delay_flights_tbl
        |WHERE origin = 'JFK'
        |""".stripMargin)

    spark.sql("show tables").show(false)

    spark.sql("""select * from us_origin_airport_jfk_tmp_view """).show()

    spark.sql("""DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view""")

    spark.sql("show tables").show()


    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show(false)
    spark.catalog.listColumns("us_delay_flights_tbl").show(false)

    println("cacheamos")
    spark.sql("""CACHE TABLE us_delay_flights_tbl""")

    spark.sql("""select * from us_delay_flights_tbl""").show()

    println("descacheamos")
    spark.sql("""UNCACHE TABLE us_delay_flights_tbl""")

    spark.sql("""select * from us_delay_flights_tbl""").show()

    // Stop the SparkSession
    spark.close()
  }
}