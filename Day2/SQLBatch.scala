package com.jpmc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
object extCustA {
  
def main(args: Array[String]){
println("SQL Spark Configuration")
val conf = new SparkConf()
//Set the logical and user defined Name of this Application
conf.setAppName("Custom extract via Sql Example")  

println("Creating Spark Context")
val ctx = new SparkContext(conf)
val sqlContext = new HiveContext(ctx)
println(" SQL Context ==> " + sqlContext);

val result = sqlContext.sql("SELECT id, name, city FROM customer")
result.show()
}
}

