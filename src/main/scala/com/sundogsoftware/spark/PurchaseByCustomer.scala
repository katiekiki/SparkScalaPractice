package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PurchaseByCustomer {

  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val customerID = fields(0)
    val amount = fields(2).toFloat
    // Create a tuple that is our result.
    (customerID, amount)
  }


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "PurchaseByCustomer")

    // Load each line of my book into an RDD
    val lines = sc.textFile("../customer-orders.csv")

    val customerOrders = lines.map(parseLine)

    val customerTotalPurchase = customerOrders.reduceByKey((x,y) => x+y)

    val customerTotalPurchaseSorted = customerTotalPurchase.map(x => (x._2, x._1)).sortByKey()

    customerTotalPurchaseSorted.foreach(println)
//    for (customerPurchase <- customerTotalPurchaseSorted) {
//       val customer = customerPurchase._2
//       val totalPurchase = customerPurchase._1
//
//       println(s"$customer : $totalPurchase")
//    }
  }

}
