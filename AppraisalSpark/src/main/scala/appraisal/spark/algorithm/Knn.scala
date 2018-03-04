package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import appraisal.spark.util.Util

object Knn {
  
  def knn(fidf: DataFrame, k: Int): DataFrame = {
    
    val lIdIndex = fidf.columns.indexOf("lineId")
    val oValIndex = fidf.columns.indexOf("originalValue")
    val ignoreIndex : Array[Int] = Array(lIdIndex, oValIndex)
    
    val cidf = fidf.filter(_.get(oValIndex) != null).cache()
    
    val rdf = fidf.rdd.map(row => (row.getLong(lIdIndex), row.getDouble(oValIndex), knn(row, cidf, k, ignoreIndex))).map(Row(_))
    
    val schema = StructType(List(StructField("lineId", LongType, nullable = true),
                                StructField("originalValue", DoubleType, nullable = true),
                                StructField("imputationValue", DoubleType, nullable = true)))
    fidf.sqlContext.createDataFrame(rdf, schema)
    
  }
  
  def knn(row: Row, cidf: Dataset[Row], k: Int, ignoreIndex : Array[Int]): Double = {
    
    val oValIndex = cidf.columns.indexOf("originalValue")
    
    if(row.get(oValIndex) != null)
      return row.getDouble(oValIndex)
    
    val dist = Util.euclidianDist(row, cidf, ignoreIndex).sortBy(_._3, false)
    
    dist.take(k).map(_._2).reduce((x,y) => x + y) / k
    
  }
  
  def run(idf: DataFrame, k: Int, attribute: String, attributes: Array[String]): Entities.ImputationResult = {
    
    var fidf = idf.withColumn("lineId", monotonically_increasing_id)
    
    var rlist = ListBuffer[String]()
    var columns = fidf.columns
    
    for(i <- 0 to (columns.length - 1)){
      
      if(!columns(i).equals(attribute) && !columns(i).equals("lineId")){
        
        if(!attributes.contains(columns(i))){
          
          rlist += columns(i)
        
        }else{
          
          fidf = fidf.filter(r => r.get(i) != null && Util.isNumeric(r.get(i).toString())).withColumn(columns(i), Util.toDouble(col(columns(i))))
          
        }
      }
         
    }
    
    rlist.toList.foreach(l => fidf = fidf.drop(l))
    fidf = fidf.withColumn("originalValue", Util.toDouble(col(attribute))).drop(attribute)
    
    var rdf = knn(fidf, k)
    
    rdf.createOrReplaceTempView("result")
    val result = rdf.sqlContext.sql("select lineid, originalValue, imputationValue from result where originalValue is null").rdd
    
    Entities.ImputationResult(result.map(r => Entities.Result(r.getLong(0), Some(r.getAs[Double](1)), r.getAs[Double](2))))
    
  }
  
}