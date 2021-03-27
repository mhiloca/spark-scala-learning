package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, StructType, StringType, FloatType, IntegerType}
import org.apache.spark.sql.functions.{min, max}

object MinTemperatureDS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Temperature(stationId: String, date: Int, measureType: String, temperature: Float)

  val session = SparkSession
    .builder
    .appName("MinTemperatureDS")
    .master("local[*]")
    .getOrCreate()

  val temperatureSchema = StructType(Array(
    StructField("stationId", StringType),
    StructField("date", IntegerType),
    StructField("measureType", StringType),
    StructField("temperature", FloatType)
  ))

  import session.implicits._
  val temperatures = session
    .read
    .schema(temperatureSchema)
    .csv("data/1800.csv")
    .as[Temperature]

  val minTemperatures = temperatures
    .withColumn("celsius", $"temperature" / 10.0)
    .filter($"measureType" === "TMIN")
    .select($"stationId", $"celsius")
    .groupBy($"stationId")
    .agg(min("celsius").alias("minTemperature"))

  val maxTemperature = temperatures
    .withColumn("celsius", $"temperature" / 10)
    .filter($"measureType" === "TMAX")
    .select($"stationId", $"celsius")
    .groupBy($"stationId")
    .agg(max("celsius").alias("maxTemperature"))

  minTemperatures.show()
  maxTemperature.show()

  session.stop()
}
