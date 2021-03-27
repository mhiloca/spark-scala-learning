package part3_dfds

import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object MyDegreesOfSeparation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","MyDegreesOfSeparation")

  val startCharacterID = 5306 // Spiderman
  val targetCharacterID = 14 // ADAM 3,031

  // we make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter : Option[LongAccumulator] = Some(sc.longAccumulator("MyDegreesOfSeparation"))

  // BFSData contains an array of hero ID connections, the distance and color
  type BFSData = (Array[Int], Int, String)

  // BFSNode has a heroID and the BFSData associated with it
  type BFSNode = (Int, BFSData)

  /** Converts a line of row input into a BFSNode */
  def convertToBFS(line: String): BFSNode = {
    var color: String = "WHITE"
    var distance: Int = 9999

    val fields = line.split("\\s+")
    val heroID = fields.head.toInt
    val connections = fields.tail.map(_.toInt)

    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }

    val bfsData: BFSData = (connections, distance, color)
    (heroID, bfsData)
  }

  /** Create "iteration 0" of our RdDD of BFSNodes */
  def createStartingRDD(sc: SparkContext): RDD[BFSNode] = {
    val inputFile = sc.textFile("data/marvel-graph.txt")
    inputFile.map(convertToBFS)
  }

  var iterationRDD = createStartingRDD(sc)


}
