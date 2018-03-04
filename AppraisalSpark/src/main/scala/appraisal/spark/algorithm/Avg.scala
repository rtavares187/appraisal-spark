package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.util.Util

object Avg {
  
  def run(idf: DataFrame, attribute: String): Entities.ImputationResult = {
    
    var attIndex = idf.columns.indexOf(attribute)
    
    val fidf = idf.withColumn("lineId", monotonically_increasing_id).filter(r => r.get(attIndex) != null && Util.isNumeric(r.get(attIndex).toString()))
    
    fidf.createOrReplaceTempView("originaldb")
    val avgValue = fidf.sqlContext.sql("select avg(" + attribute + ") from originaldb o where o." + attribute + " is not null").head().getAs[Double](0)
    
    val rdf = fidf.withColumn("originalValue", Util.toDouble(col(attribute))).drop(attribute)
    .withColumn("imputationValue", when(col("originalValue").isNotNull, col("originalValue")).otherwise(avgValue))
    
    rdf.createOrReplaceTempView("result")
    val result = rdf.sqlContext.sql("select lineid, originalValue, imputationValue from result where originalValue is null").rdd
    
    Entities.ImputationResult(result.map(r => Entities.Result(r.getLong(0), Some(r.getAs[Double](1)), r.getAs[Double](2))))
    
  }
  
}