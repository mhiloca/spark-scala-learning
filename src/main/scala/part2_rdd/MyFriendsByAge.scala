package part2_rdd

import org.apache.spark._
import org.apache.log4j._

object MyFriendsByAge extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MyFriendsByAge")

  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  val fakeFriendsRDD = sc.textFile("data/fakefriends.csv")
    .filter(_(0).isDigit)
    .map(parseLine)

  val totalByAgeRDD = fakeFriendsRDD
    .mapValues(numFriends => (numFriends, 1))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  val avgFriendsByAgeRDD = totalByAgeRDD.mapValues(x => x._1 / x._2)

  avgFriendsByAgeRDD
    .collect()
    .sorted // sorts by key
    .take(5)
    .foreach(println)

}
