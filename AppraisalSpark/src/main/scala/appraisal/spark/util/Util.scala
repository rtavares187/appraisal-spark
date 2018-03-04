package appraisal.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.rdd._

object Util {
  
  val toDouble = udf[Option[Double], String](x => if(x != null) Some(x.toDouble) else None)
  
  val toLong = udf[Long, String](_.toLong)
  
  def isNumeric(str:String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")
  
  def euclidianDist(row: Row, rowc: Row, ignoreIndex : Array[Int], ccCount: Int): Double = {
    
    var dist = 0d
    
    for(i <- 0 to ccCount){
      
      if(!ignoreIndex.contains(i)){
        
        dist += scala.math.pow(row.getDouble(i) - rowc.getDouble(i), 2) 
        
      }
      
    }
    
    return scala.math.sqrt(dist)
    
  }
  
  def euclidianDist(row: Row, cidf: DataFrame, ignoreIndex : Array[Int]): RDD[(Long, Double, Double)] = {
    
    val lIdIndex = cidf.columns.indexOf("lineId")
    val oValIndex = cidf.columns.indexOf("originalValue")
    val ccCount = cidf.columns.length
    
    cidf.rdd.map(rowc => (rowc.getLong(lIdIndex), rowc.getDouble(oValIndex) , euclidianDist(row, rowc, ignoreIndex, ccCount)))
    
  }  
  
}