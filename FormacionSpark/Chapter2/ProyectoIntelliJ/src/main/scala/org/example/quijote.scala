package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql._

/**
 * @author ${David Moreno}
 */


// APP del QUIJOTE
object quijote {
  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("Elquijote")
      .getOrCreate()
    // Cogemos el artchivo txt
    val nombreArchivo = "el_quijote.txt"
    // Leemos ela archivo
    val texto  = spark.read.text(nombreArchivo)

    println("veamos un pedacion te txto:\n")
    texto.show(5,false)

    // Vamos a probar algunos atributos
    println("Vamos a ver la salida del head")
    val b = texto.head(5)

    println("Salida del tail")
    texto.tail(3)


    println("Salida del take")
    texto.take(5)

    println("Salida del fisrt0")
    println("La primera linea es: " + texto.first())

    println("Salida del count")
    val a = texto.count()
    println("Hay un total de " + a + " palabras")

    println("Probamos el collect")
    val lineas = texto.collect()


    // Stop the SparkSession
    spark.stop()
  }
}