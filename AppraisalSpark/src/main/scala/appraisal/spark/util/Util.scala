package appraisal.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.broadcast._
import appraisal.spark.entities._

object Util {
  
  val toDouble = udf[Option[Double], String](x => if(x != null) Some(x.toDouble) else null)
  
  val toLong = udf[Long, String](_.toLong)
  
  def isNumeric(str:String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")
  
  def isNumericOrNull(obj:Any): Boolean = {
    
    if(obj == null) return true
    
    obj.toString().matches("[-+]?\\d+(\\.\\d+)?")
    
  }
  
  def euclidianDist(row: Row, rowc: Row, cColPos : Array[Int]): Double = {
    
    var dist = 0d
    
    cColPos.foreach(attIndex => {
      
      dist += scala.math.pow(row.getDouble(attIndex) - rowc.getDouble(attIndex), 2)  
      
    })
    
    return scala.math.sqrt(dist)
    
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
  
  def hasNullatColumn(df: DataFrame, feature: String): Boolean = {
    
    df.createOrReplaceTempView("imputationdb")
    val nullCount = df.sqlContext.sql("select count(*) from imputationdb where " + feature + " is null").head().getAs[Long](0)
    nullCount > 0
    
  }
  
  def getCurrentTime() = {
    
    val dateFormatter = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    var submittedDateConvert = new java.util.Date()
    dateFormatter.format(submittedDateConvert)
    
  }
  
  def getCurrentTime(date: java.util.Date) = {
    
    val dateFormatter = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    dateFormatter.format(date)
    
  }
  
  def combineResult(results: Seq[Entities.ImputationResult]): Entities.ImputationResult = {
    
    val r = results
    
    val count = results.size
    
    val k = r.map(_.k.asInstanceOf[Int]).reduce((x,y) => x + y) / count
    val totalError = r.map(_.totalError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgError = r.map(_.avgError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    val avgPercentualError = r.map(_.avgPercentError.asInstanceOf[Double]).reduce((x,y) => x + y) / count
    
    Entities.ImputationResult(null, k.intValue(), avgError, totalError, avgPercentualError, null)
    
  }
  
}