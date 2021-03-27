package part3_dfds


import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{explode, lower, split}

object WordCountDS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Book(value: String)

  val session = SparkSession
    .builder
    .appName("WordCountDS")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._
  val wordCountDS = session
    .read
    .text("data/book.txt")
    .as[Book]

  val words = wordCountDS
    .select(explode(split($"value", "\\W+")).alias("word"))
    .filter($"word" =!= "")

  val lowerCaseWords = words.select(lower($"word").alias("word"))

  val wordCount = lowerCaseWords
    .groupBy($"word").count()
    .sort($"count".desc)

//  wordCount.show(wordCount.count.toInt) // to show the whole ds

  // load in RDD and transform it into a DS
  val wordsRDD = session
    .sparkContext
    .textFile("data/book.txt")
    .flatMap(_.split("\\W+"))

  val wordsDS = wordsRDD.toDS()
  val lowerCaseWordsDS = wordsDS
    .select(lower($"value").alias("words"))
    .filter($"words" =!= "")

  val wordCount2 = lowerCaseWordsDS
    .groupBy($"words")
    .count()
    .sort($"count".desc)

  wordCount2.show()

  session.stop()
}
