package appraisal.spark.algorithm

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.entities._
import appraisal.spark.util.Util
import appraisal.spark.interfaces.ClusteringAlgorithm
import scala.collection.mutable.HashMap

object KMeans extends ClusteringAlgorithm {
  
  def run(idf: DataFrame, attribute: String, params: HashMap[String, Any] = null): Entities.ClusteringResult = {
    
    val attributes: Array[String] = params("attributes").asInstanceOf[Array[String]]
    val k: Int =  params("k").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]
    
    val removeCol = idf.columns.diff(attributes)
    val remidf = idf.drop(removeCol: _*)
    
    val context = remidf.sparkSession.sparkContext
    
    val calcCol = attributes.drop(attributes.indexOf(attribute))
    
    val fidf = context.broadcast(Util.filterNullAndNonNumeric(remidf, calcCol))
    
    val attributeIndex = fidf.value.columns.indexOf(attribute)
    
    val vectorsRdd = fidf.value.rdd.map(row => {
      
      var fLength = fidf.value.columns.length - 2
      var values = new Array[Double](fLength)
      var index = -1
      
      for(i <- 0 to fLength){
        
        if(i != attributeIndex){
          
          index += 1
          values(index) = row.getString(i).toDouble
          
        }
        
      }
        
      (row.getLong(fLength + 1), Vectors.dense(values))
      
    }).cache()
    
    val vectors = vectorsRdd.map(_._2)
    
    val kMeansModel = org.apache.spark.mllib.clustering.KMeans.train(vectors, k, maxIter)
    
    val wssse = kMeansModel.computeCost(vectors)
    
    val lineIdIndex = fidf.value.columns.length - 1
    
    val res = vectorsRdd.map(x => Entities.CResult(kMeansModel.predict(x._2), x._1))
    
    Entities.ClusteringResult(res, Some(k), Some(wssse))
    
  }
  
}