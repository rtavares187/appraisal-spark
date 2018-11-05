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

object ImputationPlanBaggingExec extends Serializable {
  
  def main(args: Array[String]) {
    
    var parallelExecution = true
    
    if(args != null && args.length > 0 && "single".equalsIgnoreCase(args(0)))
      parallelExecution = false
    
    val wallStartTime = new java.util.Date()
    Logger.getLogger(getClass.getName).error("Appraisal Spark - Wall start time: " + appraisal.spark.util.Util.getCurrentTime(wallStartTime))
    Logger.getLogger(getClass.getName).error("Parallel execution: " + parallelExecution)
    
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
      
      val features = Util.breastcancer_features
      //val features = Util.aidsocurrence_features
        
      /*  
      val features = Array[String](
        //"code_number",
        "clump_thickness",
        "uniformity_of_cell_size")  
      */  
      var feature = ""
        
      var odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
      //var odf = Util.loadAidsOccurenceAndDeath(spark).withColumn("lineId", monotonically_increasing_id)
      
      Logger.getLogger(getClass.getName).error("Data count: " + odf.count())
      
      //val k_nsqrt = scala.math.sqrt(odf.value.count()).intValue()
      val kn = odf.count().intValue()
      
      var imputationPlans = List.empty[(String, Double, Double, Int, Bagging)]
      
      val missingRate = Seq(10d, 20d, 30d)
      //val missingRate = Seq(10d)
      
      val selectionReduction = Seq(10d, 20d, 30d)
      //val selectionReduction = Seq(10d)
      
      val bT = Seq(1,2,3)
      //val bT = Seq(1)
      
      features.foreach(feat => {
        
        feature = feat
        odf = odf.withColumn("originalValue", col(feature))
        
        missingRate.foreach(mr => {
          
          val idf = new Eraser().run(odf, feature, mr)
          
          selectionReduction.foreach(sr => {
            
            bT.foreach(T => {
              
              // selection -> clustering -> regression
            
            var impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            var impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
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
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanAvg, T))
            
            var imputationParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanKnn, T))
            
            
            
            // clustering -> selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
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
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanKnn, T))
            
            
            
            // clustering -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
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
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanAvg, T))
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanKnn, T))
            
            
            
            // selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
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
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanKnn, T))
            
            
            
            // regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanAvg, T))
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, T, new Bagging(impPlanKnn, T))
              
            })
            
          })
          
        })
        
      })
      
      //val imputationPlansRdd = spark.sparkContext.parallelize(imputationPlans)
      //imputationPlansRdd.map(plan => (plan.planName, plan.run())).foreach(println(_))
      
      val planCount = imputationPlans.size
      var qPlan = planCount
      
      var resultList = List.empty[(String, Double, Double, Int, Double)]
      
      if(parallelExecution){
        
        imputationPlans.par.foreach(plan => {
        
          var execResult = plan._5.run()
          
          resultList = resultList :+ (plan._5.planName, plan._2, plan._3, plan._4, execResult.avgPercentError)
          
          qPlan -= 1
          val rPlan = planCount - qPlan
          val percC = (100 - ((100 * qPlan) / planCount))
          
          Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
        
        })
        
      }else{
      
        imputationPlans.foreach(plan => {
          
          var execResult = plan._5.run()
          
          resultList = resultList :+ (plan._5.planName, plan._2, plan._3, plan._4, execResult.avgPercentError)
          
          qPlan -= 1
          val rPlan = planCount - qPlan
          val percC = (100 - ((100 * qPlan) / planCount))
          
          Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
          
        })
        
      }
      
      val execPlanNames = resultList.map(_._1).distinct
      
      var consResult = List.empty[(String, Double, Double, Int, Double)]
      
      missingRate.foreach(mr => {
        
        selectionReduction.foreach(sr => {
          
          execPlanNames.foreach(planName => {
            
            bT.foreach(T => {
            
              val conRes = resultList.filter(x => x._1.equals(planName) && x._2 == mr && x._3 == sr && x._4 == T)
              val count = conRes.size
              val avgPlanError = conRes.map(_._5).reduce(_ + _) / count
              
              consResult = consResult :+ (planName, mr, sr, T, avgPlanError)
            
            })
            
          })
          
        })
        
      })
      
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error(" ------ CONSOLIDATED RESULT ------ ")
      
      consResult.foreach(x => {
        
        Logger.getLogger(getClass.getName).error("Plan: " + x._1 + "	" + "Missing rate: " + x._2 + "	" 
                                                + "Selection reduction: " + x._3 + "	" + "T(Bagging): " + x._4 + "	" 
                                                + "Error: " + x._5 + "%") 
        
      })
      
      val bestPlan = consResult.sortBy(_._4).head
      
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("Best plan: " + bestPlan._1 + "	" + "Missing rate: " + bestPlan._2 + "	" 
                                                + "Selection reduction: " + bestPlan._3 + "	" + "Error: " + bestPlan._4 + "%")
      
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