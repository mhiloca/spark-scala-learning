package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import scala.io.{Codec, Source}

object MostPopularMovieNicerDS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Movies(userId: Int, movieId: Int, rating: Int, timestamp: Long)

  /* Load up a Map of movie Ids to movie names */
  def loadMovieNames(): mutable.Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") // This is the current encoding of the u.item not UTF-8

    val movieNames = mutable.Map[Int, String]()
    val lines = Source.fromFile("data/ml-100k/u.item")
    lines.getLines().foreach {
      line =>
        val fields = line.split("\\|")
        if (fields.length > 1) {
          movieNames += (fields(0).toInt -> fields(1))
        }
    }
    lines.close()
    movieNames
  }

  val session = SparkSession
    .builder()
    .appName("MostPopularMovieNicerDS")
    .master("local[*]")
    .getOrCreate()


  val nameDict = session.sparkContext.broadcast(loadMovieNames())

  val moviesSchema = StructType(Array(
    StructField("userId", IntegerType),
    StructField("movieId", IntegerType),
    StructField("rating", IntegerType),
    StructField("timestamp", LongType)
  ))

  import session.implicits._
  val movies = session.read
    .option("delimiter", "\t")
    .schema(moviesSchema)
    .csv("data/ml-100k/u.data")
    .as[Movies]

  val movieCounts = movies.groupBy($"movieId").count()


  val lookupName: Int => String = (movieId: Int) => nameDict.value(movieId)
  val lookupNameUDF = udf(lookupName)

  val moviesWithNames = movieCounts
    .withColumn("movieTitle", lookupNameUDF($"movieId"))
    .select($"movieId", $"movieTitle", $"count")
    .sort($"count".desc)

  moviesWithNames.show(moviesWithNames.count.toInt, false)

  session.stop()

}
