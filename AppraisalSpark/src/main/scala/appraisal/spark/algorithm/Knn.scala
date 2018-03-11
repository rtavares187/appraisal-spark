package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import org.apache.spark.broadcast._
import scala.collection.mutable.ListBuffer
import appraisal.spark.util.Util

object Knn {
  
  def knn(fidf: DataFrame, k: Int): RDD[(Long, Option[Double], Double)] = {
    
    val lIdIndex = fidf.columns.indexOf("lineId")
    val oValIndex = fidf.columns.indexOf("originalValue")
    val ignoreIndex : Array[Int] = Array(lIdIndex, oValIndex)
    
    val context = fidf.sparkSession.sparkContext
    val cidf = context.broadcast(fidf.filter(_.get(oValIndex) != null).toDF())
    
    val rdf :RDD[(Long, Option[Double], Double)] = fidf.rdd.map(row => {
      
      val lineId = row.getLong(lIdIndex)
      val originalValue :Option[Double] = if(row.get(oValIndex) != null) Some(row.getDouble(oValIndex)) else null
      val imputationValue = if(row.get(oValIndex) != null) row.getDouble(oValIndex) else knn(row, cidf, k, ignoreIndex)
      
      (lineId, 
       originalValue, 
       imputationValue)})
    
    rdf
    
  }
  
  def knn(row: Row, cidf: Broadcast[DataFrame], k: Int, ignoreIndex : Array[Int]): Double = {
    
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
          
          fidf = Util.filterNullAndNonNumericByAtt(fidf, i).withColumn(columns(i), Util.toDouble(col(columns(i))))
          
        }
      }
         
    }
    
    rlist.toList.foreach(l => fidf = fidf.drop(l))
    
    fidf = fidf.withColumn("originalValue", Util.toDouble(col(attribute))).drop(attribute)
    
    val rdf = knn(fidf, k).filter(_._2 == null)
    
    Entities.ImputationResult(rdf.map(r => Entities.Result(r._1, r._2, r._3)))
    
  }
  
}