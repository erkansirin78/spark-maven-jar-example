package local.vbo
import org.apache.spark.sql.{SparkSession, functions => F}
import sys.process._
import java.net.URL
import java.io.File
import org.apache.log4j.{Level, Logger}

object SparkMavenExample extends App{
  // Set log level
  Logger.getLogger("org").setLevel(Level.INFO)

  // Create Spark Session
  val spark = SparkSession.builder()
    .appName("Spark Maven Example")
    .master("local[2]")
    .getOrCreate()

  // The function for downloading the Advertising.csv file
  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
  // Download the file
  fileDownloader("https://raw.githubusercontent.com/erkansirin78/datasets/master/Advertising.csv",
    "/tmp/Advertising.csv")

  // Read the file
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep",",")
    .load("file:///tmp/Advertising.csv")

  // Show 5 rows
  df.show(5)

  // Stop Spark Session
  spark.stop()

}
