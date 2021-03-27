package part2_rdd

import org.apache.spark._
import org.apache.log4j.{Logger, Level}

object MyWordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MyWordCount")

  val bookRDD = sc.textFile("data/book.txt")
    .flatMap(line => line.split("\\W+"))

  val wordRegex = """[A-Za-z]+""".r
  val words = bookRDD.filter(wordRegex.findAllIn(_).nonEmpty).map(_.toLowerCase())
  val distinctWordCount = words.countByValue()

  implicit val orderingWordCount: Ordering[(String, Long)] = Ordering.fromLessThan((a, b) => a._2 > b._2)
  val distinctWordsCountSorted = distinctWordCount.toSeq.sorted

  val wordCounts = words.map((_, 1)).reduceByKey((x, y) => x + y)
  val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

  wordCountsSorted.foreach(println)


}
