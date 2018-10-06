package appraisal.spark.entities

import appraisal.spark.interfaces._
import org.apache.spark.sql._
import appraisal.spark.strategies._

class ImputationPlan {
  
  val strategies = List.empty[AppraisalStrategy]
  
  def addStrategy(strategy :AppraisalStrategy) = {
    
    strategies :+ strategy
    
  }
  
  def run(idf: DataFrame) :Entities.ImputationResult = {
    
    var edf = idf
    var features = edf.columns
    
    for (strategy <- strategies){
      
      if (strategy.isInstanceOf[SelectionStrategy]) {
        
        val selectionResult = strategy.asInstanceOf[SelectionStrategy].run(edf);
        val useColumns = selectionResult.result.map(_.attribute).collect()
        
        edf = edf.drop(features.diff(useColumns) :_*)
        features = edf.columns
        
      }
      
      if (strategy.isInstanceOf[ClusteringStrategy]) {
        
        val cs = strategy.asInstanceOf[ClusteringStrategy]
        cs.params.update("features", features)
        val cr = cs.run(edf)
        
      }
      
      if (strategy.isInstanceOf[ImputationStrategy]) {
        
        
        
      }
      
    }
    
    null
    
  }
  
}