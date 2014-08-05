/*** SimpleApp.scala ***/
import org.apache.spark._
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/mahtab.singh/projects/spark/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/Users/mahtab.singh/projects/spark",
    List("target/scala-2.10/simple-project_2.10-0.1.jar"))
    //val conf = new SparkConf().setAppName("SimpleApp")
    //val sc = new SparkContext(conf)    
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

