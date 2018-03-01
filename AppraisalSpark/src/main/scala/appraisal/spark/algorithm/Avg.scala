package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Avg {
  
  val toDouble = udf[Option[Double], String](x => if(x != null) Some(x.toDouble) else None)
  val toLong = udf[Long, String](_.toLong)
  
  def run(idf: DataFrame, attribute: String): Entities.ImputationResult = {
    
    idf.createOrReplaceTempView("originaldb")
    
    val avgValue = idf.sqlContext.sql("select avg(" + attribute + ") from originaldb o where o." + attribute + " is not null").head().getAs[Double](0)
    
    val columndf = idf.sqlContext.sql("select " + attribute  + " as originalValue from originaldb").withColumn("originalValue", toDouble(col("originalValue")))
    
    val rdf = columndf.withColumn("imputationValue", when(col("originalValue").isNotNull, toDouble(col("originalValue"))).otherwise(avgValue))
    .withColumn("lineId", monotonically_increasing_id).rdd.filter(row => row.get(0) == null)
    
    Entities.ImputationResult(rdf.map(r => Entities.Result(r.getLong(2), Some(r.getAs[Double](0)), r.getAs[Double](1))))
    
  }
  
}