package appraisal.spark.entities

import org.apache.spark.rdd._

object Entities {
  
  final case class Result(lineId: Long, originalValue: Option[Double], imputationValue: Double,
      error: Option[Double] = null, percentualError: Option[Double] = null)
  
  final case class ImputationResult(result: RDD[Result], avgError: Option[Double] = null,
      totalError: Option[Double] = null, avgPercentError: Option[Double] = null)
  
}