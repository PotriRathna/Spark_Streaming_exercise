package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
object TextFilestream extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[*]").setAppName("Text File Stream")
  val ssc = new StreamingContext(conf, Seconds(5))
  val streamRDD= ssc.textFileStream("C:/Users/pc/IdeaProjects/filestream_input")
  val data= service.numberoflines_file(streamRDD)
  data.print()
  ssc.start()
  ssc.awaitTermination()
}
