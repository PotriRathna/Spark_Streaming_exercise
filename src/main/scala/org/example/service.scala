package org.example
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object service {

  def count(lines: ReceiverInputDStream[String]): DStream[(String, Int)]={
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts
  }

  def numberoflines_file(streamRDD:DStream[String]): DStream[Long] =
  {
    val filterdata= streamRDD.filter(!_.matches("\\w+"))
    val groupdata = filterdata.map{
      row =>
        val rowelements= row.split(",")
        (rowelements(0),row)
    }.groupByKey().count()
    groupdata
  }
}
