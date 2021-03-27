package part2_rdd

import org.apache.spark._
import org.apache.log4j.{Logger, Level}

object AmountSpentByCustomer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "AmountSpentByCustomer")

  def parseLines(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toDouble
    (customerId, amount)
  }

  val customersRDD = sc.textFile("data/customer-orders.csv")
    .map(parseLines)

  implicit val customerAmountOrdering: Ordering[(Int, Double)] = Ordering.fromLessThan((x, y) => x._2 > y._2)

  val totalAmountByCustomerRDD = customersRDD
    .reduceByKey((x, y) => x + y)
    .sortBy(row => row._2, ascending = false)

  totalAmountByCustomerRDD
    .collect()
    .foreach { row => println(f"Customer: ${row._1} -> $$${row._2}%.2f") }

}
