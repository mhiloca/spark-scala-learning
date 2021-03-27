package part3_dfds

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MostObscureSuperheroes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession
    .builder()
    .appName("MostObscureSuperhero")
    .master("local[*]")
    .getOrCreate()


  case class Connections(id: Int, connections: Int)
  case class SuperheroName(id: Int, name: String)

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

  val connections = session
    .sparkContext
    .textFile("data/Marvel-graph.txt")
    .map{line =>
      val id = line.split(" ").head.toInt
      val connections = line.split(" ").tail.length
      (id, connections)
    }
    .toDF("id", "connections")
    .as[Connections]

  val minConnectionCount = connections
    .agg(min($"connections").alias("minConnection"))
    .first()
    .getAs[Int]("minConnection")

  val obscureSuperheroes = connections
    .filter($"connections" === minConnectionCount)
    .join(superheroNames, "id")
    .select($"name")

  obscureSuperheroes.show(obscureSuperheroes.count.toInt, false)

  session.stop()

}
