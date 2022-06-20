package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
/**
 * @author ${David Moreno}
 */

object EjercicioSanFrancisco {
  // Para quitar los warnings
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  // Definimos el método principal
  def main(args : Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("ejercicioSanFrancisco")
      .getOrCreate()
    //spark.sparkContext.setLogLevel("WARN")
    // Esto solo si trabajo con datasets
    // import spark.implicits._

    /*
    COMIENZO EL EJERCICIO
    */

    // Veo una primera parte del csv para ver el schema que debo crear
    val sampleDF = spark.read
      .option("samplingRatio",0.001)
      .option("header",true)
      .csv("sf-fire-calls.csv")

    sampleDF.show(3)

    // Creación del schema
    val fire_schema = StructType(Array(
      StructField("CallNumber", IntegerType,true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", IntegerType, true),
      StructField("Box", IntegerType, true),
      StructField("OriginalPriority", IntegerType, true),
      StructField("Priority", IntegerType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", IntegerType, true),
      StructField("SupervisorDistrict", IntegerType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location",StringType,true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))

    // Importacion de los datos
    val df = spark.read.schema(fire_schema)
      .option("header",true)
      .option("sep",",")
      .csv("sf-fire-calls.csv")

    df.printSchema()

    /*
    Pregunta 1
    Encontrar los distintos motivos de llamada en 2018
     */
    println("Pregunta 1")

    val p1f1 = df.select("CallType","CallDate")
      .withColumn("Fecha",to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")
      .withColumn("Anio",year(col("Fecha")))
    p1f1.show(3)

    val p1f2 = p1f1.select("CallType")
      .where(col("Anio")===2018)
      .distinct()
    p1f2.show(5,false)

    /*
    Pregunta 2
    ¿Qué mes del año 2018 tuvo más llamadas?
     */
    println("Pregunta 2")

    val p2f1 = df.select("CallType", "CallDate")
      .withColumn("Fecha", to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")
      .withColumn("Anio", year(col("Fecha")))
      .withColumn("Mes", month(col("Fecha")))

    p2f1.show(5,false)

    val p2f2 = p2f1.select("Mes")
      .where(col("Anio")===2018)
      .groupBy("Mes")
      .count()
      .withColumnRenamed("count","Cantidad")
      .orderBy(col("Cantidad") desc)
    p2f2.show(3)

    /*
    Pregunta 3
    ¿Qué barrio genera m´s llamadas en 2018?
     */
    println("Pregunta 3")

    df.select("Neighborhood","CallDate")
      .withColumn("Fecha",to_timestamp(col("CallDate"),"MM/dd/yyyy"))
      .withColumn("Anio", year(col("Fecha")))
      .where(col("Anio")===2018)
      .groupBy("Neighborhood")
      .count()
      .withColumnRenamed("count","Cantidad")
      .orderBy(col("Cantidad") desc)
      .show(3)
    /*
    Pregunta 4
    ¿Qué barriotiene peor media de tiempo de respuesta?
    */
    println("Pregunta 4")

    val p4 = df.select("Neighborhood","CallDate","Delay")
          .withColumn("Fecha",to_timestamp(col("CallDate"),"MM/dd/yyyy"))
          .withColumn("Anio",year(col("Fecha")))
    p4.show(2)

    val p4f2 = p4.select("Neighborhood","Delay")
      .where(col("Anio")===2018)
      .groupBy("Neighborhood")
      .avg("Delay")
      .withColumnRenamed("avg(Delay)","Media")
      .orderBy(col("Media") desc)
    p4f2.show(2)

    /*
        Pregunta 5
        ¿qué semana del anio 2018 tuvo mas llamadas?
     */
    println("Pregunta 5")
    val p5 = df.select("CallDate")
      .withColumn("Fecha",to_timestamp(col("CallDate"),"MM/dd/yyyy"))
      .withColumn("Anio",year(col("Fecha")))

    val p5f2 = p5.select("Fecha")
      .where(col("Anio")===2018)
      .withColumn("week_of_year", weekofyear(col("Fecha")))
      .withColumnRenamed("week_of_year","Semana")
      .groupBy("Semana")
      .count()
      .withColumnRenamed("count","Cantidad")
      .orderBy(col("Cantidad") desc)
    p5f2.show(3)

    /* PRegunta 6
    Hay correlacion entre barrio, zip code y numero de llamdas?
     */
    println("Pregunta 6")
    val p6 = df.select("Neighborhood","ZipCode")
      .groupBy("Neighborhood","ZipCode")
      .count()
    p6.show()

    println("La correlacion entre zipcode y numero de llamadas es de : " + p6.stat.corr("ZipCode","count"))

    println("Por ultimo, guardamos en formato parquet.")

    df.write.format("parquet").save("EndToEndDF")

    spark.close()
  }
}