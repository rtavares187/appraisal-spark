package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.AppraisalStrategy
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.broadcast._

class ImputationStrategy(var params: HashMap[String, Any] = null, var imputationAlgorithm: ImputationAlgorithm) extends AppraisalStrategy {
  
  def run(idf: Broadcast[DataFrame]): Entities.ImputationResult = {
    imputationAlgorithm.run(idf, params)
  }
  
  def combineResult(results: org.apache.spark.rdd.RDD[Entities.ImputationResult]): Entities.ImputationResult = {
    
    val count = results.count()
    
    val totalError = results.map(_.totalError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgError = results.map(_.avgError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgPercentualError = results.map(_.avgPercentError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    
    Entities.ImputationResult(null, Some(avgError), Some(totalError), Some(avgPercentualError))
    
  }
  
  def algName(): String = {
    imputationAlgorithm.name()
  }
  
  def strategyName: String = {"Imputation"}
  
  def parameters: String = {params.toString()}
  
}