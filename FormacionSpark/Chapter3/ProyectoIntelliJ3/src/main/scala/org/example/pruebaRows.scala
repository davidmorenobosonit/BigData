package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Row


/**
 * @author ${user.name}
 */

object pruebaRows {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[1]")
      .appName("rows")
      .getOrCreate()
    // creamos el row
    val filablog = Row(6, "Reynold", "Xin", "https//tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))
    val filablog2 = Row(5,"agatha","christie","https//youtube.com",777,"31/3/2012",Array("Instagram","LinkedIn"))

    println("el primer elemento es " + filablog(1))
    val rows2 = Seq(filablog,filablog2)


    println(rows2)
    spark.close()
  }
}