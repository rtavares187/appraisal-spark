package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.AppraisalStrategy
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities

class SelectionStrategy(var params: HashMap[String, Any] = null, var selectionAlgorithm: SelectionAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame): Entities.SelectionResult = {
    selectionAlgorithm.run(idf, params)
  }
  
}