package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

object KMeansPlus {
  
  def run(idf: DataFrame, attribute: String, params: HashMap[String, Any] = null): Entities.ClusteringResult = {
    
    val attributes: Array[String] = params("attributes").asInstanceOf[Array[String]]
    val kLimit: Int =  params("kLimit").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]
    
    var clusteringResult: Entities.ClusteringResult = null
    
    var lastWssse: (Double, Entities.ClusteringResult) = (0d, null)
    
    for(k <- 2 to kLimit){
      
      val _params = params + ("k" -> k)
      
      clusteringResult = KMeans.run(idf, attribute, params)
      
      if(k == 2 || clusteringResult.wssse.get < lastWssse._1){
        
        lastWssse = (clusteringResult.wssse.get, clusteringResult)
        
      }else{
        
        return lastWssse._2
        
      }
      
    }
    
    null
    
  }
  
}