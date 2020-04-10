// Databricks notebook source
import org.apache.spark.sql.SparkSession

object movie_analysis {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("movies").getOrCreate()
    
    spark.stop()
  }
}
