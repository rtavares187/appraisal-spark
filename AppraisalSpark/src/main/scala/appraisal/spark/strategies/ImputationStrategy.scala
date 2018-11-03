package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.AppraisalStrategy
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.broadcast._

class ImputationStrategy(var params: HashMap[String, Any] = null, var imputationAlgorithm: ImputationAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame): Entities.ImputationResult = {
    imputationAlgorithm.run(idf, params)
  }
  
  def combineResult(results: Seq[Entities.ImputationResult]): Entities.ImputationResult = {
    
    val r = results
    
    val count = results.size
    
    val k = r.map(_.k.asInstanceOf[Int]).reduce((x,y) => x + y) / count
    val totalError = r.map(_.totalError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgError = r.map(_.avgError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgPercentualError = r.map(_.avgPercentError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    
    Entities.ImputationResult(null, k.intValue(), avgError, totalError, avgPercentualError)
    
  }
  
  def algName(): String = {
    imputationAlgorithm.name()
  }
  
  def strategyName: String = {"Imputation"}
  
  def parameters: String = {params.toString()}
  
}