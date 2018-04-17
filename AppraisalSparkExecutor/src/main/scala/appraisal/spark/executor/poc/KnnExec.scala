package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.Knn
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import appraisal.spark.statistic._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap

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
      
      val idf = Eraser.run(df, attributes(1), percent._1).withColumn("lineId", monotonically_increasing_id)
      
      val params: HashMap[String, Any] = HashMap("k" -> 10, "attributes" -> attributes)
      
      val imputationResult = Knn.run(idf, attributes(1), params)
      
      val sImputationResult = Statistic.statisticInfo(df, attributes(1), imputationResult)
      
      sImputationResult.result.foreach(Logger.getLogger("appraisal").info(_))
      
      Logger.getLogger("appraisal").info("totalError: " + sImputationResult.totalError)
      Logger.getLogger("appraisal").info("avgError: " + sImputationResult.avgError)
      Logger.getLogger("appraisal").info("avgPercentError: " + sImputationResult.avgPercentError)
      
    }catch{
      
      case ex : Exception => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}