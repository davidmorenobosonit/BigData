package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author ${David Moreno}
 */

object reduce {
  // Evitamos warnings innecesarios
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  // Creamos la SparkSession
  def main(args : Array[String]) {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder().master("local[1]")
      .appName("reduce")
      .getOrCreate()
    import spark.implicits._

    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")

    spark.sql(""" select celsius,
                     reduce(celsius,
                            0,
                            (t,acc) -> t + acc ,
                            acc     -> (acc div size(celsius) * 9 div 5) + 32
                            ) as avgFah
              from tC
          """).show(false)

    // Stop the SparkSession
    spark.close()
  }
}