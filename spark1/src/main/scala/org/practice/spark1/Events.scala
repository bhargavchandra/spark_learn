
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Event(organizer: String, name: String, budget: Int)


object Events {
  def main(args:Array[String])={
     val inputFile=args(0)
     val outputfile=args(1)
   
    
    val conf=new SparkConf().setAppName("rdd operations").setMaster("local")

    val sc=new SparkContext(conf)
    
     val rdd = sc.textFile(inputFile).map(clean)
     
    
    val pair_rdd=rdd.map(x=>(x.organizer,x.budget))
    pair_rdd.reduceByKey(_ +_).saveAsTextFile(outputfile)
}
  
  def clean(line:String)={
    var arr=line.split(",")
    new Event(arr(0),arr(1),arr(2).toInt)
  }
}