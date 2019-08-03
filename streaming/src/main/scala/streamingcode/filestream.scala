
package org.inceptez.streaming.streamingcode

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext

object filestream {
def main(args:Array[String]){
  val sparkconf = new SparkConf().setAppName("FileStreaming").setMaster("local[*]")
  val sparkcontext = new SparkContext(sparkconf)
  sparkcontext.setLogLevel("ERROR")
  
  val ssc=new StreamingContext(sparkcontext, Seconds(10))
  val lines=ssc.textFileStream("file:///home/hduser/sparkdata/streaming/")
  val courses=lines.flatMap(_.split(" "))
  val coursesCounts=courses.map(x=>(x,1)).reduceByKey(_+_)
  coursesCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
}