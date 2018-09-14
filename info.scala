package org.practice.caseclass
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class cars(make:String,Model:String,MPG:Integer,Cylinders:Integer,Engine_Disp:Integer,Horsepower:Integer,Weight:Integer,Accelerate:Double, Year:Integer, Origin:String)

object info {
  
  def main(args:Array[String])={
    
      val inputFile=args(0)
     val outputfile1=args(1)
     val outputfile2=args(2)
   
    
    val conf=new SparkConf().setAppName("rdd operations").setMaster("local")

    val sc=new SparkContext(conf)
      
      val rdd=sc.textFile(inputFile).map(clean)
      rdd.cache();
   
      
     
     
      
   //  val american_cars_info=rdd.filter(extract).saveAsTextFile(outputfile1)
     
     val pair_rdd_make_weights=rdd.map(x=>(x.make,x.Weight))
     val weights_of_same_make=pair_rdd_make_weights.reduceByKey(_ + _).saveAsTextFile(outputfile2)
     
  }
  def clean(line:String)={
        var x=line.split("\t")
        (new cars(x(0).toString,x(1).toString,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt,x(7).toDouble,x(8).toInt,x(9).toString))
        }
  
  def extract(x:cars)={
    x.Origin=="American"
      }
}