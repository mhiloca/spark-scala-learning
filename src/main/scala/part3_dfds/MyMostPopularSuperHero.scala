package part3_dfds

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, Level}

object MyMostPopularSuperHero extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class SuperheroName(id: Int, name: String)

  val session = SparkSession
    .builder()
    .appName("MostPopularSuperHero")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  val superheroConnectionRows = session.read.text("data/Marvel-graph.txt")
  val superheroNamesRows = session.read.text("data/Marvel-names.txt")

  val getConnections = udf((superheros: String) => superheros.split(" ").tail.length)
  val getNames = udf((superhero: String) =>
    superhero.split(" ").tail.mkString.replace("\"", ""))

  val superHeroConnections = superheroConnectionRows
    .withColumn("superhero", split($"value", " ")(0))
    .withColumn("connections", getConnections($"value"))
    .groupBy($"superhero").agg(sum($"connections").as("numberOfConnections"))
    .select($"superhero", $"numberOfConnections")
    .sort($"numberOfConnections".desc)

  val mostPopularSuperhero = superHeroConnections.limit(1).first().getAs[String]("superhero")

  val mostPopularSuperheroName = superheroNamesRows
    .withColumn("superhero", split($"value", " ")(0))
    .withColumn("name", getNames($"value"))
    .select($"superhero", $"name")
    .filter($"superhero" === mostPopularSuperhero)

  mostPopularSuperheroName.show(false)

  session.stop()
}
