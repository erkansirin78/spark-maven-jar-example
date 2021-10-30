package local.vbo

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object SparkMavenExample extends App{
  println("Hello Spark Maven!")
  Logger.getLogger("org").setLevel(Level.INFO)

  val spark = SparkSession.builder()
    .appName("Spark Maven Example")
    .master("local[2]")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep",",")
    .load("file:///home/train/datasets/Advertising.csv")

  df.show(5)

}
