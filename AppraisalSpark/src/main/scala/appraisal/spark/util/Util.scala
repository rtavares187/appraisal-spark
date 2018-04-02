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
  
  def euclidianDist(row: Row, rowc: Row, cColPos : Array[Int]): Double = {
    
    var dist = 0d
    
    cColPos.foreach(attIndex => {
      
      dist += scala.math.pow(row.getDouble(attIndex) - rowc.getDouble(attIndex), 2)  
      
    })
    
    return scala.math.sqrt(dist)
    
  }
  
  def euclidianDist(row: Row, cidf: Broadcast[DataFrame], calcCol: Array[String]): RDD[(Long, Double, Double)] = {
    
    val ecidf = cidf.value
    
    val lIdIndex = ecidf.columns.indexOf("lineId")
    val oValIndex = ecidf.columns.indexOf("originalValue")
    val cColPos = calcCol.map(ecidf.columns.indexOf(_))
    
    ecidf.rdd.map(rowc => (rowc.getLong(lIdIndex), rowc.getDouble(oValIndex) , euclidianDist(row, rowc, cColPos)))
    
  }
  
  def filterNullAndNonNumeric(df: DataFrame, columns: Array[String] = null): DataFrame = {
    
    var _columns = df.columns
    if(columns != null)
      _columns = columns
    
    var rdf = df
    
    _columns.foreach(column => {
      
      val columnIndex = rdf.columns.indexOf(column)
      rdf = filterNullAndNonNumericByAtt(rdf, columnIndex)
        
    })
    
    rdf
      
  }
  
  def filterNullAndNonNumericByAtt(df: DataFrame, attIndex: Int): DataFrame = {
    
    df.filter(r => r.get(attIndex) != null && Util.isNumeric(r.get(attIndex).toString()))
  
  }
  
}