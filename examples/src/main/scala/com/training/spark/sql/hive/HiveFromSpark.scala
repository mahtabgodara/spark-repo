
package com.training.spark.examples.sql.hive

import com.google.common.io.{ByteStreams, Files}

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object HiveFromSpark {
  case class Record(key: Int, value: String)
  // Copy kv1.txt file from classpath to temporary directory
  val kv1Stream = HiveFromSpark.getClass.getResourceAsStream("/kv1.txt")
  val kv1File = File.createTempFile("kv1", "txt")
  kv1File.deleteOnExit()
  ByteStreams.copy(kv1Stream, Files.newOutputStreamSupplier(kv1File))

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveFromSpark")
    sparkConf.setMaster("local")
    sparkConf.set("spark.driver.memory","2g")
    sparkConf.set("spark.executor.extraJavaOptions","-XX:MaxPermSize=512m")
    val sc = new SparkContext(sparkConf)

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    val hiveContext = new HiveContext(sc)
    import hiveContext._

    sql("CREATE EXTERNAL TABLE IF NOT EXISTS src (key INT, value STRING) LOCATION 'hdfs://localhost:54310/hive/src'")
    sql(s"LOAD DATA LOCAL INPATH '${kv1File.getAbsolutePath}' INTO TABLE src")

    // Queries are expressed in HiveQL
    println("Result of 'SELECT *': ")
    sql("SELECT * FROM src").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sql("SELECT COUNT(*) FROM src").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    println("Result of RDD.map:")
    val rddAsStrings = rddFromSql.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    // You can also register RDDs as temporary tables within a HiveContext.
    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    rdd.registerTempTable("records")

    // Queries can then join RDD data with data stored in Hive.
    println("Result of SELECT *:")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").collect().foreach(println)

    sc.stop()
  }
}
