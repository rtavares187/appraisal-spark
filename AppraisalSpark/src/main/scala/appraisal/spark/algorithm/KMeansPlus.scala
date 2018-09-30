package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

object KMeansPlus {
  
  def run(idf: DataFrame, attribute: String, params: Map[String, Any] = null): Entities.ClusteringResult = {
    
    val attributes: Array[String] = params("attributes").asInstanceOf[Array[String]]
    val k: Int = params("k").asInstanceOf[Int]
    val kLimit: Int =  params("kLimit").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]
    
    var clusteringResult: Entities.ClusteringResult = null
    
    var lastWssse: (Double, Entities.ClusteringResult) = (0d, null)
    
    for(_k <- k to kLimit){
      
      val _params: Map[String, Any] = params + ("k" -> _k)
      
      clusteringResult = KMeans.run(idf, attribute, _params)
      
      if(_k == k || clusteringResult.wssse.get < lastWssse._1){
        
        lastWssse = (clusteringResult.wssse.get, clusteringResult)
        
      }else{
        
        return lastWssse._2
        
      }
      
    }
    
    null
    
  }
  
}