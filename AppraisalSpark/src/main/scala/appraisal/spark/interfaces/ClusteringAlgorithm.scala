package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

trait ClusteringAlgorithm {
  
  def run(idf: DataFrame, attribute: String, params: Map[String, Any] = null): Entities.ClusteringResult
  
}