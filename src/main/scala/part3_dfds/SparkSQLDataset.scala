package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Logger, Level}

object SparkSQLDataset extends App {

  case class Person(id: Int, name: String, age: Int, friend: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  val peopleDS = session.read
    .option("header", "true")
    .schema(Encoders.product[Person].schema)
    .csv("data/fakefriends.csv")
    .as[Person]

  peopleDS.printSchema()
  peopleDS.createOrReplaceTempView("people")

  val teenagers = session.sql("SELECT * FROM people WHERE age <= 19")

  teenagers.show(truncate = false)

  session.stop()
}
