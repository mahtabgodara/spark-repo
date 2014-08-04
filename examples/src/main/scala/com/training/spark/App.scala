package com.training.spark
import org.apache.spark._
/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val logFile = "/home/hduser/projects/spark/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/home/hduser/projects/spark",
    List("/home/hduser/projects/sparkProject/examples/target/examples-0.0.1.jar"))
    //val conf = new SparkConf().setAppName("SimpleApp")
    //val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
