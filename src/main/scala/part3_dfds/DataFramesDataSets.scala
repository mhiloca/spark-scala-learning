package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Logger, Level}

object DataFramesDataSets extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(id: Int, name: String, age: Int, friends: Int)

  val session = SparkSession
    .builder
    .appName("DataFramesDataSets")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._
  val people = session.read
    .option("header", "true")
    .schema(Encoders.product[Person].schema)
    .csv("data/fakefriends.csv")
    .as[Person]

  people.select($"name").show()

  people.filter($"age" > 21).show()

  people.groupBy($"age").count().show()

  people.select($"name", $"age" + 10).show()

  session.stop()
}
