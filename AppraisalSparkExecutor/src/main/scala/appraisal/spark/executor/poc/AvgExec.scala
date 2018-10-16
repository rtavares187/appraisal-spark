package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.eraser.Eraser
import appraisal.spark.entities._
import appraisal.spark.algorithm._
import appraisal.spark.statistic._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap

object AvgExec {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      val df = spark.sparkContext.broadcast(Util.loadBreastCancer(spark))
      
      val percent = (10, 20, 30, 40, 50)
      
      val features = Array[String](
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
      
      val idf = spark.sparkContext.broadcast(new Eraser().run(df, features(1), percent._1).withColumn("lineId", monotonically_increasing_id))
      
      val params: HashMap[String, Any] = HashMap(
          "imputationFeature" -> features(1))
      
      val imputationResult = new Avg().run(idf, params)
      
      val sImputationResult = Statistic.statisticInfo(spark.sparkContext.broadcast(df.value.withColumn("lineId", monotonically_increasing_id)), features(1), imputationResult)
      
      sImputationResult.result.foreach(Logger.getLogger("appraisal").error(_))
      
      Logger.getLogger("appraisal").error("totalError: " + sImputationResult.totalError)
      Logger.getLogger("appraisal").error("avgError: " + sImputationResult.avgError)
      Logger.getLogger("appraisal").error("avgPercentError: " + sImputationResult.avgPercentError)
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}