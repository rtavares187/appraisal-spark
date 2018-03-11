package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.Knn
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import appraisal.spark.statistic._

object KnnExec {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      var df = Util.loadBreastCancer(spark)
      
      val percent = (10, 20, 30, 40, 50)
      
      val attributes = Array[String](
          //"code_number",
          "clump_thickness",
          "uniformity_of_cell_size",
          "uniformity_of_cell_shape",
          "marginal_adhesion",
          "single_epithelial_cell_size",
          "bare_nuclei",
          "bland_chromatin",
          "normal_nucleoli",
          "mitoses",
          "class")
      
      val idf = Eraser.run(df, attributes(1), percent._1)
      
      val imputationResult = Knn.run(idf, 10, attributes(1), attributes)
      
      imputationResult.result.foreach(println(_))
      
      val sImputationResult = Statistic.statisticInfo(df, attributes(1), imputationResult)
      
      sImputationResult.result.foreach(println(_))
      
      println("totalError: " + sImputationResult.totalError)
      println("averageError: " + sImputationResult.avgError)
      println("avgPercentError: " + sImputationResult.avgPercentError)
      
    }catch{
      
      case ex : Throwable => println(ex)
      
    }
    
  }
  
}