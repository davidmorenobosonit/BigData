package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ${user.name}
 */

object AuthorAge {
  def main(args : Array[String]) {
    val spark = SparkSession.builder().master("local[1]")
      .appName("AuthorAge")
      .getOrCreate()
    // creamos el dataFrame
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)))
      .toDF("Nombre", "Edad")

    mediaedad(dataDF)
    spark.close()
  }

  // Creo la media
  def mediaedad(df:DataFrame): Unit = {
    val avdDF = df.groupBy("Nombre").agg(avg("Edad"))
    avdDF.show()
  }
}