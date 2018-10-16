package appraisal.spark.statistic

import appraisal.spark.entities._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import appraisal.spark.util.Util
import java.text.DecimalFormat
import org.apache.spark.broadcast._

object Statistic {
  
  def statisticInfo(lods :Broadcast[DataFrame], attribute: String, impres :Entities.ImputationResult) :Entities.ImputationResult = {
    
    val impds = impres.result.map(r => Row(r.lineId, r.originalValue, r.imputationValue))
    
    val schema = StructType(List(StructField("lineId", LongType, nullable = true),
                                StructField("originalValue", DoubleType, nullable = true),
                                StructField("imputationValue", DoubleType, nullable = true)))
                                
    val impdf = lods.value.sqlContext.createDataFrame(impds, schema)
    
    impdf.createOrReplaceTempView("result")
    lods.value.createOrReplaceTempView("original")
    
    val result = impdf.sqlContext.sql("select r.lineid, o." + attribute + " as originalValue, r.imputationValue " + 
                                      "from result r inner join original o on r.lineid = o.lineid order by r.lineid")
                                      .withColumn("originalValue", Util.toDouble(col("originalValue"))).rdd
    
    val impResult = Entities.ImputationResult(result.map(r => {
      
      val error = scala.math.sqrt(scala.math.pow(r.getDouble(2) - r.getDouble(1), 2)).doubleValue()
      val percentualError = ((error / r.getDouble(1)) * 100).doubleValue()
      
      Entities.Result(r.getLong(0), Some(r.getDouble(1)), r.getDouble(2), Some(error), Some(percentualError))
      
    }))
    
    val count = impResult.result.count()
    val totalError = impResult.result.map(_.error.head).reduce(_ + _)
    val avgError = (totalError / count).doubleValue()
    val avgPercentualError = (impResult.result.map(_.percentualError.head).reduce(_ + _) / count).doubleValue()
    
    Entities.ImputationResult(impResult.result, Some(avgError), Some(totalError), Some(avgPercentualError))
    
  }
  
}