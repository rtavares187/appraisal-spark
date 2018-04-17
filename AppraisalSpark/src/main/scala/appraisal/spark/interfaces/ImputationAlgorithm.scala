package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

trait ImputationAlgorithm {
  
  def run(idf: DataFrame, attribute: String, params: HashMap[String, Any] = null): Entities.ImputationResult
  
}