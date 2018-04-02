package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._

trait SelectionAlgorithm {
  
  def run(idf: DataFrame, attribute: String, attributes: Array[String], percent: Double): Entities.SelectionResult
  
}