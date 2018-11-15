package com.evenkat 
import org.apache.spark.{SparkConf, SparkContext} 
object ScalaFirst { 
  def main(args: Array[String]){ 
    //Scala Main Method 
	println("Creating Spark Configuration") 
//Create an Object of Spark Configuration
val conf = new SparkConf() 
//Set the logical and user defined Name of this Application
conf.setAppName("My First Spark Scala Application") 

println ( "Creating Spark Context" ) 
//Create a Spark Context and provide previously created 
//Object of SparkConf as an reference. 
val ctx  =  new  SparkContext( conf ) 

println ( "Loading the Dataset and will further process it") 

val file  = "/input/sample"

val logData  = ctx.textFile(file, 2) 

val numLines  = logData.filter(line => true).count() 
//Finally Printing the Number of lines.
println("Number of Lines in the Dataset "+numLines) 
  } 
} 