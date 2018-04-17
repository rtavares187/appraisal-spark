package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import org.apache.spark.broadcast._
import scala.collection.mutable.ListBuffer
import appraisal.spark.util.Util
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.ImputationAlgorithm

object Knn extends ImputationAlgorithm {
  
  def run(idf: DataFrame, attribute: String, params: HashMap[String, Any] = null): Entities.ImputationResult = {
    
    val k: Int = params("k").asInstanceOf[Int]
    val attributes: Array[String] = params("attributes").asInstanceOf[Array[String]]
    
    val removeCol = idf.columns.diff(attributes).filter(!_.equals("lineId"))
    val calcCol = attributes.filter(!_.equals(attribute))
    
    var fidf = Util.filterNullAndNonNumeric(idf.drop(removeCol: _*), calcCol)
    
    attributes.foreach(att => fidf = fidf.withColumn(att, Util.toDouble(col(att))))
    
    fidf = fidf.withColumn("originalValue", col(attribute)).drop(attribute)
    
    val rdf = knn(fidf, k, calcCol).filter(_._2 == null)
    
    Entities.ImputationResult(rdf.map(r => Entities.Result(r._1, r._2, r._3)))
    
  }
  
  def knn(fidf: DataFrame, k: Int, calcCol: Array[String]): RDD[(Long, Option[Double], Double)] = {
    
    val lIdIndex = fidf.columns.indexOf("lineId")
    val oValIndex = fidf.columns.indexOf("originalValue")
    
    val context = fidf.sparkSession.sparkContext
    val cidf = context.broadcast(fidf.filter(_.get(oValIndex) != null).toDF())
    
    val rdf :RDD[(Long, Option[Double], Double)] = fidf.rdd.map(row => {
      
      val lineId = row.getLong(lIdIndex)
      val originalValue :Option[Double] = if(row.get(oValIndex) != null) Some(row.getDouble(oValIndex)) else null
      val imputationValue = if(row.get(oValIndex) != null) row.getDouble(oValIndex) else knn(row, cidf, k, calcCol)
      
      (lineId, 
       originalValue, 
       imputationValue)})
    
    rdf
    
  }
  
  def knn(row: Row, cidf: Broadcast[DataFrame], k: Int, calcCol: Array[String]): Double = {
    
    val dist = Util.euclidianDist(row, cidf, calcCol).sortBy(_._3)
    
    dist.take(k).map(_._2).reduce((x,y) => x + y) / k
    
  }
  
}