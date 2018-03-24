package appraisal.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.broadcast._
import appraisal.spark.entities._

object Util {
  
  val toDouble = udf[Option[Double], String](x => if(x != null) Some(x.toDouble) else None)
  
  val toLong = udf[Long, String](_.toLong)
  
  def isNumeric(str:String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")
  
  def euclidianDist(row: Row, rowc: Row, ignoreIndex : Array[Int], ccCount: Int): Double = {
    
    var dist = 0d
    
    for(i <- 0 to (ccCount - 1)){
      
      if(!ignoreIndex.contains(i)){
        
        dist += scala.math.pow(row.getDouble(i) - rowc.getDouble(i), 2) 
        
      }
      
    }
    
    return scala.math.sqrt(dist)
    
  }
  
  def euclidianDist(row: Row, cidf: Broadcast[DataFrame], ignoreIndex : Array[Int]): RDD[(Long, Double, Double)] = {
    
    val ecidf = cidf.value
    
    val lIdIndex = ecidf.columns.indexOf("lineId")
    val oValIndex = ecidf.columns.indexOf("originalValue")
    val ccCount = ecidf.columns.length
    
    ecidf.rdd.map(rowc => (rowc.getLong(lIdIndex), rowc.getDouble(oValIndex) , euclidianDist(row, rowc, ignoreIndex, ccCount)))
    
  }
  
  def filterNullAndNonNumeric(df: DataFrame, ignoreColumns: Option[Array[String]] = Some(Array(""))): DataFrame = {
    
    var rdf = df
    
    for(i <- 0 to (df.columns.length - 1)){
      
      if(!ignoreColumns.contains(df.columns(i)))
        rdf = rdf.filter(r => r.get(i) != null && Util.isNumeric(r.get(i).toString()))
      
    }
    
    rdf
      
  }
  
  def filterNullAndNonNumericByAtt(df: DataFrame, attIndex: Int): DataFrame = {
    
    df.filter(r => r.get(attIndex) != null && Util.isNumeric(r.get(attIndex).toString()))
  
  }
  
}