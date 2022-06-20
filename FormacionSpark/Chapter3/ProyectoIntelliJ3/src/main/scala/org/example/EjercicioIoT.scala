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

object EjercicioIoT {
  // Para quitar los warnings
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  // Definimos el método principal
  def main(args : Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("ejIoT")
      .getOrCreate()

    import spark.implicits._

    /*
    COMIENZO EL EJERCICIO
    */

    val ds = spark.read
      .json("iot_devices.json")
      .as[DeviceIoTData2]

    println("Vamos a vevr un poco del DATASET")

    ds.show(4,false)

    logg.warn("Vamos a hacer un filtro")

    val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})
    filterTempDS.show(5, false)

    logg.warn("Hacemos las actividades")
    ds.select("*")
      .where(col("temp")>30 && col("humidity")>70)
      .show(3)

    ds.select("temp","device_name","device_id","cca3")
      .where(col("temp")>25)
      .show(5)

    ds.select("battery_level","device_id")
      .where(col("battery_level")<10 )
      .show(10)

    ds.select("cn","c02_level")
      .groupBy("cn")
      .avg("c02_level")
      .withColumnRenamed("avg(c02_level)","media")
      .orderBy(col("media") desc)
      .show(10)


    spark.close()
  }
}

case class DeviceIoTData2 (battery_level: Long, c02_level: Long, cca2: String, cca3: String,
                          cn: String, device_id: Long, device_name: String, humidity: Long,
                          ip: String, latitude: Double, lcd: String, longitude: Double,
                          scale: String, temp: Long, timestamp: Long)