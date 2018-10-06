package appraisal.spark.interfaces
import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.entities._

trait AppraisalStrategy {
  
  def run(idf: DataFrame): StrategyResult
  
}