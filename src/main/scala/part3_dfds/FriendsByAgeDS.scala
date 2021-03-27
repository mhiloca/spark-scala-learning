package part3_dfds


import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, round}

object FriendsByAgeDS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(id: Int, name: String, age: Int, friends: Int)

  val session = SparkSession
    .builder
    .appName("FriendsByAgeDS")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._
  val people = session.read
    .option("header", "true")
    .schema(Encoders.product[Person].schema)
    .csv("data/fakefriends.csv")
    .as[Person]

  val avgFriendsByAge = people
    .groupBy($"age")
    .agg(round(avg("friends"), 2).alias("avg_friends"))


  avgFriendsByAge.sort($"age").show()

  session.stop()

}
