package org.practice.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rddoperations {
  
  def main(args:Array[String])={
    
    val conf=new SparkConf().setAppName("rdd operations").setMaster("local")

    val sc=new SparkContext(conf)
    val input=sc.textFile("input.txt")
    println("trans1 called")
    val words=input.flatMap(x=>(x.split(" ")))
    val words_tuple=words.map(x=>(x,1))
    
    val frequency=words_tuple.reduceByKey(_+_)
    frequency.saveAsTextFile("output.txt")
    
    
    
        

  }
  
}