package appraisal.spark.algorithm

import appraisal.spark.interfaces.SelectionAlgorithm
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.entities._
import appraisal.spark.util.Util

object Pca extends SelectionAlgorithm {
  
  def run(idf: DataFrame, attribute: String, attributes: Array[String], percent: Double): Entities.SelectionResult = {
    
    val removeCol = idf.columns.diff(attributes)
    val remidf = idf.drop(removeCol: _*)
    
    val context = remidf.sparkSession.sparkContext
    
    val fidf = context.broadcast(Util.filterNullAndNonNumeric(remidf))
    
    val columns = fidf.value.columns
    
    val qtdAttributes = columns.length
    
    val pcq = ((1 - (percent / 100)) * qtdAttributes).intValue()
    
    val vectorsRdd = fidf.value.rdd.map(row => {
      
      val length = columns.length
      val fLength = length - 1
      var values = new Array[Double](length)
      
      for(i <- 0 to fLength)
        values(i) = row.getString(i).toDouble
        
      (Vectors.dense(values))
      
    }).cache()
    
    val matrix: RowMatrix = new RowMatrix(vectorsRdd)
    
    val cvla = matrix.computeCovariance()
    
    val attributeIndex = columns.indexOf(attribute)
    
    val sres = context.parallelize((0 to (cvla.numCols - 1)).toArray.map(l => (cvla.apply(attributeIndex, l).abs, l)))
                .filter(_._2 != attributeIndex).sortBy(_._1, false)
                .take(pcq).map(l => (columns(l._2), idf.columns.indexOf(columns(l._2)), l._1))
                .zipWithIndex.map(l => (l._2, l._1))
    
    val rddres = context.parallelize(sres).map(l => Entities.SResult(l._1, l._2._1, l._2._2, l._2._3))
    Entities.SelectionResult(rddres)
    
  }
  
}