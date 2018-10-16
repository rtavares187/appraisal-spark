package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.algorithm.Pca
import appraisal.spark.eraser.Eraser
import scala.collection.mutable.HashMap

object PcaExec {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      var df = spark.sparkContext.broadcast(Util.loadBreastCancer(spark))
      
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
          
      val percentReduction = (10d, 20d, 30d, 40d, 50d)
      
      val idf = spark.sparkContext.broadcast(new Eraser().run(df, features(1), percentReduction._1))
      
      val params: HashMap[String, Any] = HashMap(
          "features" -> features, 
          "imputationFeature" -> features(1),
          "percentReduction" -> percentReduction._3)
      
      val res = new Pca().run(idf, params)
      
      res.result.sortBy(_.index).collect().foreach(Logger.getLogger("appraisal").error(_))
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}