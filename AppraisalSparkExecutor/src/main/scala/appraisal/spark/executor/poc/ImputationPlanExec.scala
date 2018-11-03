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
    
    val wallStartTime = new java.util.Date()
    Logger.getLogger(getClass.getName).error("Appraisal Spark - Wall start time: " + appraisal.spark.util.Util.getCurrentTime(wallStartTime))
    
    try{
      
      val conf = new SparkConf()
      //.set("spark.executor.memory", "1g")
      //.set("spark.executor.cores", "8")
      .set("spark.network.timeout", "3600")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
      .set("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      
      val spark = SparkSession
        .builder
        .appName("AppraisalSpark")
        //.master("local[*]")
        //.master("spark://127.0.0.1:7077")
        .config(conf)
        .getOrCreate()
      
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
        "mitoses")
        //"class")
      
      var feature = features(1)
        
      val odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
                                      .withColumn("originalValue", col(feature))
      
      Logger.getLogger(getClass.getName).error("Data count: " + odf.count())
      
      //val k_nsqrt = scala.math.sqrt(odf.value.count()).intValue()
      val kn = odf.count().intValue()
      
      var imputationPlans = List.empty[ImputationPlan]
      
      features.foreach(feat => {
        
        feature = feat
        
        val missingRate = Seq(10d, 20d, 30d)
        //val missingRate = Seq(10d)
        
        missingRate.foreach(mr => {
          
          val idf = new Eraser().run(odf, feature, mr)
          
          val selectionReduction = Seq(10d, 20d, 30d)
          //val selectionReduction = Seq(10d)
          
          selectionReduction.foreach(sr => {
            
            // selection -> clustering -> regression
            
            var impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features)
            var impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features)
            
            var selectionParamsAvg: HashMap[String, Any] = HashMap(
            "percentReduction" -> sr)
            
            var selectionParamsKnn: HashMap[String, Any] = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            var clusteringParamsAvg: HashMap[String, Any] = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            var clusteringParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            var imputationParamsAvg: HashMap[String, Any] = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ impPlanAvg
            
            var imputationParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ impPlanKnn
            
            
            
            // clustering -> selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features)
            
            clusteringParamsAvg = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            clusteringParamsKnn = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            selectionParamsAvg = HashMap(
            "percentReduction" -> sr)
            
            selectionParamsKnn = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            //imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ impPlanKnn
            
            
            
            // clustering -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features)
            
            clusteringParamsAvg = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            clusteringParamsKnn = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ impPlanKnn
            
            
            
            // selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features)
            
            selectionParamsAvg = HashMap(
            "percentReduction" -> sr)
            
            selectionParamsKnn = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            //imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ impPlanKnn
            
            
            
            // regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features)
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ impPlanKnn
            
          })
          
        })
        
      })
      
      //val imputationPlansRdd = spark.sparkContext.parallelize(imputationPlans)
      //imputationPlansRdd.map(plan => (plan.planName, plan.run())).foreach(println(_))
      
      val planCount = imputationPlans.size
      var qPlan = planCount
      
      var resultList = List.empty[(String, Double, String)]
      
      imputationPlans.foreach(plan => {
        
        var execResult = plan.run()
        
        resultList = resultList :+ (plan.planName, execResult.avgPercentError, execResult.params)
        
        println(plan.planName)
        println()
        
        qPlan -= 1
        val rPlan = planCount - qPlan
        Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + ((100 * qPlan) / planCount) + "%.")
        
      }
      )
      
      val bestPlan = resultList.sortBy(_._2).head
      
      Logger.getLogger(getClass.getName).error("BEST IMPUTATION PLAN: " + bestPlan._1 + " ERROR: " + bestPlan._2 + " PARAMS: " + bestPlan._3)
      
      val wallStopTime = new java.util.Date()
    
      val wallTimeseconds   = ((wallStopTime.getTime - wallStartTime.getTime) / 1000)
      
      val wallTimesMinutes = wallTimeseconds / 60
      
      val wallTimesHours = wallTimesMinutes / 60
      
      Logger.getLogger(getClass.getName).error("------------------ Wall stop time: "
                                              + appraisal.spark.util.Util.getCurrentTime(wallStartTime)
                                              + " --- Total wall time: " + wallTimeseconds + " seconds, "
                                              + wallTimesMinutes + " minutes, "
                                              + wallTimesHours + " hours.")
      
    }catch{
      
      case ex : Exception => {
                              ex.printStackTrace()
                              Logger.getLogger(getClass.getName).error(ex)
      }
      
    }
    
  }
  
}