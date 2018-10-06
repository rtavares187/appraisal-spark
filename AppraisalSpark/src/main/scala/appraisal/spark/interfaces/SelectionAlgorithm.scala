package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

trait SelectionAlgorithm extends AppraisalAlgorithm {
  
  def run(idf: DataFrame, params: HashMap[String, Any] = null): Entities.SelectionResult
  
}