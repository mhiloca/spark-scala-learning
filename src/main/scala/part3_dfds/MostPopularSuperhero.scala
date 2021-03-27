package part3_dfds

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MostPopularSuperhero extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession
    .builder()
    .appName("MostPopularSuperhero")
    .master("local[*]")
    .getOrCreate()

  case class SuperheroName(id: Int, name: String)
  case class Superhero(value: String)

  val namesSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType)
  ))

  import session.implicits._
  val superheroNames = session
    .read
    .option("delimiter", " ")
    .schema(namesSchema)
    .csv("data/Marvel-names.txt")
    .as[SuperheroName]

  val lines = session
    .read
    .text("data/Marvel-graph.txt")
    .as[Superhero]

  val connections = lines
    .withColumn("id", split($"value", " ")(0))
    .withColumn("connections", size(split($"value", " ")) - 1)
    .groupBy($"id").agg(sum("connections").alias("connections"))

  val mostPopular = connections
    .sort($"connections".desc)
    .first()
    .getAs[String]("id")

  val mostPopularSuperhero = superheroNames.filter($"id" === mostPopular)
  mostPopularSuperhero.show(false)

  session.stop()
}
