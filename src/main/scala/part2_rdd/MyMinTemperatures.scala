package part2_rdd

import scala.math.{max, min}
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

object MyMinTemperatures extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MyMinTemperatures")

  def parseLines(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f
    (stationId, entryType, temperature)
  }

  val temperaturesRDD = sc.textFile("data/1800.csv")
    .map(parseLines)

  val minTemperatures = temperaturesRDD.filter(_._2 == "TMIN")
  val stationAndTemperature = minTemperatures.map(x => (x._1, x._3.toFloat))

  val minTempsByStation = stationAndTemperature.reduceByKey((x, y) => min(x, y))
  val maxTempsByStation = stationAndTemperature.reduceByKey((x, y) => max(x, y))

  stationAndTemperature.keys.distinct().collect().foreach(println)
  minTempsByStation.collect().foreach(println)
  maxTempsByStation.collect().foreach(println)


}
