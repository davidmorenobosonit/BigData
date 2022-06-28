package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.Random._

/**
 * @author ${user.name}
 */

case class Bloggers(id:BigInt, first:String, last:String, url:String, published:String,hits:BigInt, campaigns:Array[String])
case class Usage(uuid:Int, uname:String, usage:Int)
case class UsageCost(uuid:Int, uname:String, usage:Int, cost:Double)

object CaseClasses {
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  def main(args : Array[String]) {

    val spark = SparkSession.builder().master("local[1]")
      .appName("CaseClasses")
      .getOrCreate()
    import spark.implicits._

    println("Vamos a leer el DasaSet con la estructura creada")
    val ds = spark.read.json("blogs.json").as[Bloggers]

    ds.show(3,false)

    logg.warn("A otra cosa")
   println("Vamos a crear un dataset con datos aleatorios")
    val r = new scala.util.Random(42)

    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),r.nextInt(1000)))
    val dsUsage = spark.createDataset(data)
    dsUsage.show(2)

    println("Creamos una lambda function para filtrar")
    dsUsage.filter(d=> d.usage > 500)
      .orderBy(desc("usage"))
      .show(5,false)

    println("Notese que tambien podemos crear una funcion externa y darsela al filter")
    def filterUsage(u: Usage) = u.usage > 900

    dsUsage.filter(filterUsage(_)).orderBy(desc("usage")).show(5)

    logg.warn("Precio especial")

    println("Veamos que tambien se pueden crear nuevos campos")

    dsUsage.map(u => {if (u.usage>750) u.usage* .15 else u.usage* .5}).show(5,false)

    println("Creamos un campo pero con una función.")

//Observamos que le decimos que la entrada va a ser un entero, y la salida un doble
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    // Use the function as an argument to map()
    dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

    logg.warn("Definimos la funcion y el nuevo caseclass")

    //Vamos a crear la función, donde le pasamos toda la calse y devolvemos una clase nueva.
    def calculocoste(u:Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage*.15 else u.usage*0.5
      UsageCost(u.uuid,u.uname,u.usage,v)
    }

    dsUsage.map(u =>{calculocoste(u)} ).show(5)
    spark.close()
  }
}