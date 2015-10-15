package com.training.spark.examples
import org.apache.spark._
import com.datastax.spark.connector._

object CassandraExample {
  
  def main(args : Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("local[2]", "test", conf)
    val rdd = sc.cassandraTable("test", "kv")
    
    println(rdd.count)
    println(rdd.first)
    //println(rdd.map(_.getInt("value")).sum)
    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))  
  }
}
