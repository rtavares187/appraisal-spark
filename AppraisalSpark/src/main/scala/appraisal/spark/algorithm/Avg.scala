package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.util.Util
import appraisal.spark.interfaces.ImputationAlgorithm
import scala.collection.mutable.HashMap

class Avg extends ImputationAlgorithm {
  
  def run(idf: DataFrame, params: HashMap[String, Any] = null): Entities.ImputationResult = {
    
    val attribute: String = params("imputationFeature").asInstanceOf[String] 
    
    val fidf = idf
      
    val avgidf = Util.filterNullAndNonNumericByAtt(fidf, idf.columns.indexOf(attribute))
    avgidf.createOrReplaceTempView("originaldb")
    
    val avgValue = avgidf.sqlContext.sql("select avg(" + attribute + ") from originaldb").head().getAs[Double](0)
    
    val rdf = fidf.withColumn("originalValue", Util.toDouble(col(attribute))).drop(attribute)
    .withColumn("imputationValue", when(col("originalValue").isNotNull, col("originalValue")).otherwise(avgValue))
    
    rdf.createOrReplaceTempView("result")
    val result = rdf.sqlContext.sql("select lineid, originalValue, imputationValue from result where originalValue is null").rdd
    
    Entities.ImputationResult(result.map(r => {
      
      val lineId = r.getLong(0)
      val originalValue :Option[Double] = if(r.get(1) != null) Some(r.getDouble(1)) else null
      val imputationValue = if(r.get(1) != null) r.getDouble(1) else r.getDouble(2)
      
      Entities.Result(lineId, originalValue, imputationValue)}))
    
  }
  
}