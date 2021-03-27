package playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SubFolders extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession
    .builder
    .appName("SubFolders")
    .master("local[*]")
    .getOrCreate()

  val csvsDF = session
    .read
    .option("recursiveFileLookup","true")
    .csv("data/csvs")

  println(csvsDF.count())

  session.stop()
}
