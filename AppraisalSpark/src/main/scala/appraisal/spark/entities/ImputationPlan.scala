package appraisal.spark.entities

import appraisal.spark.interfaces._
import org.apache.spark.sql._
import appraisal.spark.strategies._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, IntegerType}
import appraisal.spark.util.Util
import appraisal.spark.statistic.Statistic
import org.apache.log4j.Logger
import org.apache.spark.broadcast._

class ImputationPlan extends Serializable {
  
  val log = Logger.getLogger(getClass.getName)
  
  var strategies = List.empty[AppraisalStrategy]
  var planName = "";
  
  def addStrategy(strategy :AppraisalStrategy) = {
    
    strategies = strategies :+ strategy
    
    if(!"".equals(planName)){
      
      planName += "->"
    
    }
    
    planName += strategy.strategyName + "[" + strategy.algName() + "]"
    
  }
  
  def run(idf: Broadcast[DataFrame], odf: Broadcast[DataFrame]) :Entities.ImputationResult = {
    
    log.error("-------------------------------------")
    log.error("Running imputation plan: " + planName)
    
    var firs :Entities.ImputationResult = null;
    
    try{
    
      var edf = idf
      var features = edf.value.columns
      
      var imputationBatch :org.apache.spark.rdd.RDD[DataFrame] = null
      
      for (strategy <- strategies){
        
        if (strategy.isInstanceOf[SelectionStrategy]) {
          
          val ss = strategy.asInstanceOf[SelectionStrategy]
          
          log.error("Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + ss.parameters)
          
          val sr = ss.run(odf);
          
          log.error("SelectionResult: " + sr.toString())
          
          val useColumns = sr.result.map(_.attribute).collect()
          
          edf = edf.value.sparkSession.sparkContext.broadcast(edf.value.drop(features.diff(useColumns).filter(_ != "lineId") :_*))
          features = edf.value.columns.filter(_ != "lineId")
          
        }
        
        if (strategy.isInstanceOf[ClusteringStrategy]) {
          
          val cs = strategy.asInstanceOf[ClusteringStrategy]
          cs.params.update("features", features)
          
          log.error("Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + cs.parameters)
          
          val cr = cs.run(edf)
          
          log.error("ClusteringResult: " + cr.toString())
          
          val schema = new StructType()
          .add(StructField("cluster", IntegerType, true))
          .add(StructField("lineidcluster", LongType, true))
          
          val lineIdPos = edf.value.columns.indexOf("lineId")
          
          var cdf = edf.value.sparkSession.createDataFrame(cr.result.map(x => Row(x.cluster, x.lineId)), schema)
          var ccdf = cdf.rdd.map(r => (r.getInt(0), edf.value.filter(l => l.getLong(lineIdPos) == r.getLong(1)).first()))
          
          // refazer em sparksql
          
          imputationBatch = cdf.rdd.map(_.getInt(0)).distinct().map(x => ccdf.filter(r => r._1 == x)
              .map(_._2)).map(z => cdf.sparkSession.createDataFrame(z, edf.value.schema))
          
        }
        
        if (strategy.isInstanceOf[ImputationStrategy]) {
          
           if(imputationBatch == null) imputationBatch = edf.value.sparkSession.sparkContext.parallelize(Seq(edf.value))
            
           val is = strategy.asInstanceOf[ImputationStrategy]
           is.params.update("features", features)
           
           imputationBatch = imputationBatch.filter(df => Util.hasNullatColumn(df, is.params("imputationFeature").asInstanceOf[String])) 
           
           log.error("Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + is.parameters)
           
           val irs = imputationBatch.map(x => is.run(edf.value.sparkSession.sparkContext.broadcast(x))).map(x => Statistic.statisticInfo(odf, is.params("imputationFeature").asInstanceOf[String], x))
           
           firs = is.combineResult(irs)
           
           log.error("ImputationResult: " + firs.toString())
           log.error("totalError: " + firs.totalError)
           log.error("avgError: " + firs.avgError)
           log.error("avgPercentError: " + firs.avgPercentError)
           
        }
        
      }
      
      log.error("-------------------------------------")
      
    }catch{
      
      case ex : Exception => log.error("Error executing imputation plan: " + planName, ex)
      
    }
    
    return firs
    
  }
  
}