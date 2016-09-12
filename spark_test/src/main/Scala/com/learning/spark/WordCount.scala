package com.learning.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = { 
  
  val conf = new SparkConf().setAppName("WordCount").setMaster("local")
  
  val sc = new SparkContext(conf)
  
  val inputpath = args(0)
  val outputpath = args(1)
  
  val wc = sc.textFile(inputpath).flatMap(x => x.split(" ")).map( x =>(x,1)).reduceByKey((acc,value) => acc+ value)
  
  wc.saveAsTextFile(outputpath)
  println("#######################################################################")
  println("Word Count COMPLETED SUCCESSFULLY")
  println("GitHub uploading COMPLETED SUCCESSFULLY")
  println("#######################################################################")
  
  }
}