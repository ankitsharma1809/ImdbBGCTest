package com.bcgp.imdb.spark
import org.apache.spark.sql.SparkSession

trait SparkSessionBuilder {
 lazy implicit val spark: SparkSession = {
   SparkSession
   .builder()
   .master("local")
   .appName("SparkTest")
   .getOrCreate()
 }
}