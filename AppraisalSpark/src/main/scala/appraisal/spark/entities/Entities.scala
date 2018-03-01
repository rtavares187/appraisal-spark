package appraisal.spark.entities

import org.apache.spark.rdd._

object Entities {
  
  final case class Result(lineId: Long, originalValue: Option[Double], imputationValue: Double)
  
  final case class ImputationResult(result: RDD[Result])
  
}