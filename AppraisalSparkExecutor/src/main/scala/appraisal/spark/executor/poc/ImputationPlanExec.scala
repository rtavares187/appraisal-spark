package appraisal.spark.executor.poc

import org.apache.log4j._
import appraisal.spark.entities.ImputationPlan
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.Knn
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import appraisal.spark.strategies._
import appraisal.spark.algorithm._ 

object ImputationPlanExec extends Serializable {
  
  def main(args: Array[String]) {
    
    try{
      
      val spark = SparkSession
        .builder
        .appName("AppraisalSpark")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      var odf = spark.sparkContext.broadcast(Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id))
      
      val k_knnkmeans = scala.math.sqrt(odf.value.count()).intValue()
      
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
      
      val imputationFeature = features(1)
          
      //val missingRate = spark.sparkContext.parallelize(Seq(10d, 20d, 30d, 40d, 50d))
      val missingRate = spark.sparkContext.parallelize(Seq(10d))
      
      missingRate.foreach(mr => {
        
        Logger.getLogger(getClass.getName).error("Missing rate at " + mr + "% in feature " + imputationFeature)
        
        val idf = odf.value.sparkSession.sparkContext.broadcast(new Eraser().run(odf, imputationFeature, mr).withColumn("lineId", monotonically_increasing_id))
        
        //val selectionReduction = odf.value.sparkSession.sparkContext.parallelize(Seq(10d, 20d, 30d, 40d, 50d))
        val selectionReduction = odf.value.sparkSession.sparkContext.parallelize(Seq(10d))
        
        selectionReduction.foreach(sr => {
          
          Logger.getLogger(getClass.getName).error("Feature reduction at " + sr + "% in selection strategy")
          
          val impPlanAvg = new ImputationPlan()
          
          val selectionParams: HashMap[String, Any] = HashMap(
          "features" -> features, 
          "imputationFeature" -> imputationFeature,
          "percentReduction" -> sr)
          
          impPlanAvg.addStrategy(new SelectionStrategy(selectionParams, new Pca()))
          
          val clusteringParams: HashMap[String, Any] = HashMap(
          "features" -> features,
          "imputationFeature" -> imputationFeature,
          "k" -> 2, 
          "maxIter" -> 200, 
          "kLimit" -> k_knnkmeans)
          
          impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParams, new KMeansPlus()))
          
          val impPlanKnn = impPlanAvg
          
          val imputationParamsAvg: HashMap[String, Any] = HashMap(
          "imputationFeature" -> imputationFeature)
          
          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
          
          impPlanAvg.run(idf, odf)
          
          val imputationParamsKnn: HashMap[String, Any] = HashMap(
          "k" -> k_knnkmeans, 
          "features" -> features, 
          "imputationFeature" -> imputationFeature)
          
          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
          
          impPlanKnn.run(idf, odf)
          
        })
        
      })
      
    }catch{
      
      case ex : Exception => {
                              ex.printStackTrace()
                              Logger.getLogger(getClass.getName).error(ex)
      }
      
    }
    
  }
  
}