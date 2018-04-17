package appraisal.spark.executor.util

import org.apache.spark.sql._

object Util {
  
  def loadBreastCancer(spark:SparkSession): DataFrame = {
    
    spark.read.option("header", true).csv("C:\\data\\breast-cancer-wisconsin.data.csv")
    
  }
  
}