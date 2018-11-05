package appraisal.spark.executor.util

import org.apache.spark.sql._

object Util {
  
  val breastcancer_features = Array[String](
        //"code_number",
        "clump_thickness",
        "uniformity_of_cell_size",
        "uniformity_of_cell_shape",
        "marginal_adhesion",
        "single_epithelial_cell_size",
        "bare_nuclei",
        "bland_chromatin",
        "normal_nucleoli",
        "mitoses")
        //"class")
      
    val aidsocurrence_features = Array[String](
        //"Time",
        //"AIDS Death",
        "Regulation on the Prevention and Treatment of AIDS",
        "AIDS prevention knowledge",
        "AIDS awareness",
        "Handwritten AIDS newspaper",
        "Handwritten anti-AIDS newspaper",
        "AIDS virus",
        "How to prevent AIDS",
        "Route of transmission of AIDS",
        "AIDS prevention",
        "What is AIDS",
        "Which day is World AIDS Day",
        "The origins of the AIDS",
        "The origins of the AIDS",
        "AIDS",
        "AIDS Day",
        "The origins of AIDS",
        "How to prevent AIDS",
        "AIDS awareness day",
        "World AIDS Day",
        "How to prevent AIDS",
        "AIDS awareness slogan",
        "AIDS/HIV",
        "Initial symptoms of AIDS",
        "Images of AIDS skin rashes",
        "How long will one survive once he/she contracts HIV",
        "How long can AIDS patients survive",
        "HIV/AIDS prevention",
        "AIDS village in Henan province",
        "How does one contract HIV",
        "Number of AIDS patients in China",
        "Symptoms of AIDS in incubation period")
  
  def loadBreastCancer(spark:SparkSession): DataFrame = {
    
    spark.read.option("header", true).csv("C:\\data\\breast-cancer-wisconsin.data.csv")
    
  }
  
  def loadAidsOccurenceAndDeath(spark:SparkSession): DataFrame = {
    
    spark.read.option("header", true).csv("C:\\data\\AIDS Occurrence and Death and Queries.csv")
    
  }
  
}