package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, max}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object MostPopularMovie extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Movie(movieId: Int)

  val session = SparkSession
    .builder()
    .appName("MostPopularMovie")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  val moviesSchema = StructType(Array(
    StructField("userId", IntegerType),
    StructField("movieId", IntegerType),
    StructField("rating", IntegerType),
    StructField("timestamp", IntegerType)
  ))

  val movies = session.read
    .option("delimiter", "\t")
    .schema(moviesSchema)
    .csv("data/ml-100k/u.data")
    .as[Movie]

  val numberOfRatingsPerMovie = movies
    .select($"movieId")
    .groupBy("movieId")
    .count()
    .orderBy($"count".desc)

  numberOfRatingsPerMovie.show(10, false)

  session.stop()

}
