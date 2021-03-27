package part2_rdd

import org.apache.spark._
import org.apache.log4j._

object MyRatingsCounter extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MyRatingsCounter")

  val lines = sc.textFile("data/ml-100k/u.data")

  val ratings = lines.map(_.split("\t")(2))
  val results = ratings.countByValue()
  val sortResults = results.toSeq.sortBy(_._1)

  sortResults.reverse.foreach(println)
}
