package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.AppraisalStrategy
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities

class ClusteringStrategy(var params: HashMap[String, Any] = null, var clusteringAlgorithm: ClusteringAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame): Entities.ClusteringResult = {
    clusteringAlgorithm.run(idf, params)
  }
  
}