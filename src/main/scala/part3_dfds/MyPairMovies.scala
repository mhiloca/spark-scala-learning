package part3_dfds

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{sum, sqrt, count, when}
import org.apache.log4j.{Logger, Level}

object MyPairMovies extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Movies(userID: Int, movieID: Int, rating: Int)
  case class MovieName(movieID: Int, title: String)
  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)

  val session = SparkSession.builder()
    .appName("MyPairMovies")
    .master("local")
    .getOrCreate()

  import session.implicits._

  val ratings = session.read
    .option("delimiter", "\t")
    .schema(Encoders.product[Movies].schema)
    .csv("data/ml-100k/u.data")
    .as[Movies]

  val names = session.read.
    option("delimiter", "|")
    .option("charset", "ISO-8859-1")
    .schema(Encoders.product[MovieName].schema)
    .csv("data/ml-100k/u.item")
    .as[MovieName]

  def computeCosineSimilarity(session: SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {

    // Compute xx, xy, and yy columns
    val pairScores = data
      .withColumn("xx", $"rating1" * $"rating1")
      .withColumn("yy", $"rating2" * $"rating2")
      .withColumn("xy", $"rating1" * $"rating2")

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum($"xy").alias("numerator"),
        (sqrt(sum("xx")) * sqrt(sum($"yy"))).alias("denominator"),
        count($"xy").alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs
    val result = calculateSimilarity
      .withColumn("score",
        when($"denominator" =!= 0, $"numerator" / $"denominator")
          .otherwise(null)
      )
      .select("movie1", "movie2", "score", "numPairs")
      .as[MoviePairsSimilarity]

    result
  }

  val filteredRatings = ratings.filter($"rating" > 4)

  val moviePairs = filteredRatings.as("ratings1")
    .join(filteredRatings.as("ratings2"),
      $"ratings1.userID" === $"ratings2.userID" && $"ratings1.movieID" < $"ratings2.movieID")
    .select($"ratings1.movieID".alias("movie1"),
      $"ratings2.movieID".alias("movie2"),
      $"ratings1.rating".alias("rating1"),
      $"ratings2.rating".alias("rating2")
    )
    .as[MoviePairs]

  val moviePairsSimilarities = computeCosineSimilarity(session, moviePairs).cache()


  if (args.length > 0) {
    val scoreThreshold = 0.97
    val coOccurrenceThreshold = 50.0

    val movieID: Int = args.head.toInt

    // Filter for movies with this sim that are "good" as defined by our quality thresholds above
    val filteredResults = moviePairsSimilarities.filter(
      ($"movie1" === movieID || $"movie2" === movieID)  &&
        ($"score" > scoreThreshold && $"numPairs" > coOccurrenceThreshold)
    )

    //Sort by quality score
    val results = filteredResults.sort($"score".desc)

    results
      .join(names, $"movie2" === $"movieID")
      .withColumn("movieName", when($"movie2" === $"movieID", $"title").otherwise(null))
      .select($"movieName", $"score", $"numPairs")
      .show(false)

  }

}
