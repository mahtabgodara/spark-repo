package com.training.spark.examples
import org.apache.spark._
import org.apache.spark.SparkContext._
case class ApacheAccessLog(ipAddress: String, clientIdentd: String,
                           userId: String, dateTime: String, method: String,
                           endpoint: String, protocol: String,
                           responseCode: Int, contentSize: Long) {

}

object ApacheAccessLog {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseLogLine(log: String): ApacheAccessLog = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse log line: " + log)
    }
    val m = res.get
    ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
  }
}


object AccumulatorExample {
  
  def main(args : Array[String]) {
    val logFile = args(0)
    val conf = new SparkConf().setAppName("Accumulator Example")
    val sc = new SparkContext(conf)
    val jpg = sc.accumulator(0,"JPG")
    val gif = sc.accumulator(0,"GIF")
    val logData = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine)
    def imgCounters(log:ApacheAccessLog) = {
      val pos = log.endpoint.lastIndexOf(".")
      val ext = if (pos > -1) log.endpoint.substring(log.endpoint.lastIndexOf(".")+1, log.endpoint.length).toLowerCase else "unknown"
      ext match {
      	case "jpg" => jpg += 1
        case "gif" => gif += 1
        case _ => 
      }
    }
    logData.map(imgCounters).collect
    println("GIF counters are: "+ gif.value)
    println("JPG counters are: "+ jpg.value)
  }
}
