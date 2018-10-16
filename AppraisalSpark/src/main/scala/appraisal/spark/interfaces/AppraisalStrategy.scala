package appraisal.spark.interfaces
import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.entities._
import org.apache.spark.broadcast._

trait AppraisalStrategy extends Serializable {
  
  def run(idf: Broadcast[DataFrame]): StrategyResult
  
  def algName(): String
  
  def strategyName: String
  
  def parameters: String
  
}