package com.training.spark.examples
import org.apache.spark._
import org.apache.spark.storage.StorageLevel

object App {
  
  def main(args : Array[String]) {
    val logFile = args(0) 
    val conf = new SparkConf().setAppName("SimpleApp")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
   // sc.stop
  }

}
