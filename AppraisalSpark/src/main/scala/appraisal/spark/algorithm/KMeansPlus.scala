package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._

object KMeansPlus {
  
  def run(idf: DataFrame, attribute: String, attributes: Array[String], iterations: Int, kLimit: Int = 100): Entities.ClusteringResult = {
    
    var clusteringResult: Entities.ClusteringResult = null
    
    var lastWssse: (Double, Entities.ClusteringResult) = (0d, null)
    
    for(k <- 2 to kLimit){
      
      clusteringResult = KMeans.run(idf, attribute, attributes, k, iterations)
      
      if(k == 2 || clusteringResult.wssse.get < lastWssse._1){
        
        lastWssse = (clusteringResult.wssse.get, clusteringResult)
        
      }else{
        
        return lastWssse._2
        
      }
      
    }
    
    null
    
  }
  
}