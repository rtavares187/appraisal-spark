package appraisal.spark.entities

import appraisal.spark.interfaces._
import org.apache.spark.sql._
import appraisal.spark.strategies._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, IntegerType}

class ImputationPlan {
  
  val strategies = List.empty[AppraisalStrategy]
  
  def addStrategy(strategy :AppraisalStrategy) = {
    
    strategies :+ strategy
    
  }
  
  def run(idf: DataFrame) :Entities.ImputationResult = {
    
    var edf = idf
    var features = edf.columns
    
    for (strategy <- strategies){
      
      if (strategy.isInstanceOf[SelectionStrategy]) {
        
        val selectionResult = strategy.asInstanceOf[SelectionStrategy].run(edf);
        val useColumns = selectionResult.result.map(_.attribute).collect()
        
        edf = edf.drop(features.diff(useColumns) :_*)
        features = edf.columns
        
      }
      
      var imputationBatch :org.apache.spark.rdd.RDD[DataFrame] = null
      
      if (strategy.isInstanceOf[ClusteringStrategy]) {
        
        val cs = strategy.asInstanceOf[ClusteringStrategy]
        cs.params.update("features", features)
        val cr = cs.run(edf)
        
        val schema = new StructType()
        .add(StructField("cluster", IntegerType, true))
        .add(StructField("lineidcluster", LongType, true))
        
        val lineIdPos = edf.columns.indexOf("lineId")
        val context = edf.sparkSession.sparkContext
        val bedf = context.broadcast(edf)
        
        var cdf = edf.sparkSession.createDataFrame(cr.result.map(x => Row(x.cluster, x.lineId)), schema)
        var ccdf = cdf.rdd.map(r => (r.getInt(0), bedf.value.filter(l => l.getLong(lineIdPos) == r.getLong(1)).first()))
        
        imputationBatch = cdf.rdd.map(_.getInt(0)).distinct().map(x => ccdf.filter(r => r._1 == x)
            .map(_._2)).map(z => cdf.sparkSession.createDataFrame(z, bedf.value.schema))
        
      }
      
      if(imputationBatch == null) imputationBatch = edf.sparkSession.sparkContext.parallelize(Seq(edf))
      
      if (strategy.isInstanceOf[ImputationStrategy]) {
          
         val is = strategy.asInstanceOf[ImputationStrategy]
         is.params.update("features", features)
         
         filtrar dfs que possuam nulo na coluna imputada 
         
         val irs = imputationBatch.map(x => is.run(x))
        
      }
      
    }
    
    null
    
  }
  
}