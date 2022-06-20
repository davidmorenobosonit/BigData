package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author ${user.name}
 */

object DataSet {
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  def main(args : Array[String]) {
    //val log = Logger.getRootLogger()
    //log.setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local[1]")
      .appName("DataSet")
      .getOrCreate()
    //spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val ds = spark.read.json("iot_devices.json").as[DeviceIoTData]

    ds.show(3)

    val filterTempDS = ds.filter(d => {d.battery_level > 6 && d.humidity > 50})

    //filterTempDS.show(5, false)

    showDF(filterTempDS)

    spark.close()
  }
  def showDF(ds:Dataset[DeviceIoTData]): Unit ={
    logg.warn("Entrando en el método showDF")

    ds.show(5,false)
  }

}
case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String,
                          cn: String, device_id: Long, device_name: String, humidity: Long,
                          ip: String, latitude: Double, lcd: String, longitude: Double,
                          scale: String, temp: Long, timestamp: Long)