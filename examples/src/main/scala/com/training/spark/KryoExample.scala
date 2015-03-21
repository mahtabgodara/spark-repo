package com.training.spark.examples
import org.apache.spark._
import org.apache.spark.SparkContext._
import com.esotericsoftware.kryo._
import org.apache.spark.serializer._

class Employee(val name:String,val age:Int) //extends java.io.Serializable

object KryoExample {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("SimpleApp")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.training.spark.examples.MyKryoRegistrator")
    val sc = new SparkContext(conf)
    val emps = sc.parallelize((1 to 10000).map(x => (x, new Employee("A",30) ) ) )
    //val list = (1 to 10000).map(x => (x, "a" ) )	
    //val emps = sc.parallelize( list)
    emps.reduceByKey( (x,y) => y ).saveAsTextFile("out.1")
  }

}
