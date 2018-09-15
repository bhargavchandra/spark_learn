package or.practice.broadcast

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object orderanalysis {
  def main(args:Array[String])={
    
     
    
    val conf=new SparkConf().setAppName("rdd operations").setMaster("local")

    val sc=new SparkContext(conf)
      
      val products=sc.textFile("products.txt")

 val department=sc.textFile("department.txt")

 val category=sc.textFile("category.txt")


 val departmentsmap=department.map(x=>(x.split(",")(0).toInt,x.split(",")(1)))


 val productsmap=products.map(x=>(x.split(",")(1).toInt,x.split(",")(0).toInt))
 


 val categoriesmap=category.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt))



     


 val productcategories=productsmap.join(categoriesmap)


 productcategories.collect().foreach(println)
/*(2,(201,1001)) //(category_id or //prod_cat_id,(product_id,cat_dept_id))
(2,(201,3001))
(1,(301,1001))
(1,(301,2001))
(1,(401,1001))
(1,(401,2001))
(1,(101,1001))
(1,(101,2001))
*/
 val productcategoriesmap=productcategories.map(x=>(x._2._2,x._2._1))


 productcategoriesmap.collect().foreach(println)
/*(1001,201)
(3001,201)
(1001,301)
(2001,301)
(1001,401)
(2001,401)
(1001,101)
(2001,101)
*/
 val productcategoriesdepartments=productcategoriesmap.join(departmentsmap)


 productcategoriesdepartments.take(2)
//res19: Array[(Int, (Int, String))] = Array((1001,(201,"ABC")), (1001,(301,"ABC")))


//get into form  (product_id,department_name)
 val productdepartmentsmap=productcategoriesdepartments.map(x=>(x._2._1,x._2._2))

 
 productdepartmentsmap.take(1)
//res20: Array[(Int, String)] = Array((201,"ABC"))

 productdepartmentsmap.count
//res21: Long = 8

//convert this into hashmap and broadcast to be used as  lookup for further analysis


 val bc=sc.broadcast(productdepartmentsmap.collectAsMap())
//bc: org.apache.spark.broadcast.Broadcast[scala.collection.Map[Int,String]] = Broadcast(21)

 
                             

 val orders=sc.textFile("orders.txt")


 val orderitems=sc.textFile("orderitems.txt")


// filter orders with status completed and split into (order_id,order_date)
 val orderscompleted=orders.filter(x=>(x.split(",")(2)=="completed")).map(x=>(x.split(",")(0).toInt,x.split(",")(1)))





 orderitems.take(1)
//res22: Array[String] = Array(1,1001,200)


//get into form (order_id,get deptname from lookup using  //order_department_id of orderitems 
 val orderitemsmap=orderitems.map(x=>(x.split(",")(0).toInt,(bc.value.get(x.split(",")(1).toInt),x.split(",")(2).toFloat)))

 val ordersjoin=orderscompleted.join(orderitemsmap)
 
 ordersjoin.saveAsTextFile("ordersjoinitems")
 
  ordersjoin.take(2)
//res29: Array[(Int, (String, (Option[String], Float)))] = Array((4,(03-01-2018,(Some("DEF"),570.0))), (1,(01-01-2018,(Some("DEF"),200.0))))
 //(order_id,(order_date,(deptname,sale value)))

 
 
 //problem getsale amountperdayperdepartment so get date and //dept as keys and sale as value and do reduceByKey
 val revenueperdayperdepartment=ordersjoin.map(x=>((x._2._1,x._2._2._1),x._2._2._2)).reduceByKey(_ + _)
  revenueperdayperdepartment.sortByKey().saveAsTextFile("orderanalysis")
}
}