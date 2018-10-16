package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.ClusteringAlgorithm
import org.apache.spark.broadcast._

class KMeansPlus extends ClusteringAlgorithm {
  
  def run(idf: Broadcast[DataFrame], params: HashMap[String, Any] = null): Entities.ClusteringResult = {
    
    val k: Int = params("k").asInstanceOf[Int]
    val kLimit: Int =  params("kLimit").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]
    
    var clusteringResult: Entities.ClusteringResult = null
    
    var lastWssse: (Double, Entities.ClusteringResult) = (0d, null)
    
    for(_k <- k to kLimit){
      
      val _params: HashMap[String, Any] = params
      _params.update("k", _k)
      
      clusteringResult = new KMeans().run(idf, params)
      
      if(_k == k || clusteringResult.wssse.get < lastWssse._1){
        
        lastWssse = (clusteringResult.wssse.get, clusteringResult)
        
      }else{
        
        return lastWssse._2
        
      }
      
    }
    
    null
    
  }
  
  def name(): String = {"KMeansPlus"}
  
}