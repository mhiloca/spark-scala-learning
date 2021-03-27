package part2_rdd

import org.apache.spark._
import org.apache.log4j.{Logger, Level}

object MyRedFox extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MyForxWordCount")

  val redFoxRDD = sc.textFile("data/redfox.txt")
    .flatMap {line => line.split(" ")}

  println(redFoxRDD.collect().toList)

}
