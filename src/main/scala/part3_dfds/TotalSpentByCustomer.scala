package part3_dfds

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

object TotalSpentByCustomer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Order(customerId: Int, productId: Int, amount: Float)

  val session = SparkSession
    .builder()
    .appName("TotalSpentByCustomer")
    .master("local[*]")
    .getOrCreate()

  val orderSchema = StructType(Array(
    StructField("customerId", IntegerType),
    StructField("productId", IntegerType),
    StructField("amount", FloatType)
  ))

  import session.implicits._
  val orders = session
    .read
    .schema(orderSchema)
    .csv("data/customer-orders.csv")
    .as[Order]

  val totalSpentByCustomer = orders
    .groupBy($"customerId")
    .agg(
      round(sum("amount"), 2)
        .as("totalSpent")
    )
    .sort($"totalSpent".desc)

  totalSpentByCustomer.show(totalSpentByCustomer.count.toInt)

  session.stop()
}
