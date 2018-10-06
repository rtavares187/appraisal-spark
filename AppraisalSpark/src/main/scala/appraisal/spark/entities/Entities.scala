package appraisal.spark.entities

import org.apache.spark.rdd._
import appraisal.spark.interfaces.StrategyResult

object Entities {
  
  final case class Result(lineId: Long, originalValue: Option[Double], imputationValue: Double,
      error: Option[Double] = null, percentualError: Option[Double] = null)
  
  final case class ImputationResult(result: RDD[Result], avgError: Option[Double] = null,
      totalError: Option[Double] = null, avgPercentError: Option[Double] = null)  extends StrategyResult
  
  final case class CResult(cluster: Int, lineId: Long)
      
  final case class ClusteringResult(result: RDD[CResult], k: Option[Int] = null, wssse: Option[Double] = null) extends StrategyResult
  
  final case class SResult(index: Int, attribute: String, col: Int, covariance: Double)
  
  final case class SelectionResult(result: RDD[SResult]) extends StrategyResult
     
}