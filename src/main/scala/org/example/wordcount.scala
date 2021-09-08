package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
object wordcount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
  val ssc = new StreamingContext(conf, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 9999)
  val count =service.count(lines)
  count.print()
  ssc.start()
  ssc.awaitTermination()
}