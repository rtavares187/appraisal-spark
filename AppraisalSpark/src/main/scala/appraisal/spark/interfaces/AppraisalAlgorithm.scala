package appraisal.spark.interfaces

import scala.collection.mutable.HashMap
import org.apache.spark.sql._

trait AppraisalAlgorithm {
  
  def run(idf: DataFrame, params: HashMap[String, Any] = null): StrategyResult
  
}