package appraisal.spark.eraser

import org.apache.spark.sql._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

object Eraser {
  
  def run(odf: DataFrame, attribute: String, percent: Double): DataFrame = {
   
   var nodf = odf
   nodf = nodf.withColumn("atid", monotonically_increasing_id)
   
   val qtd = ((nodf.count() * percent) / 100).toInt
   
   nodf.createOrReplaceTempView("originaldb")
   
   val niddf = nodf.sqlContext.sql("select atid from originaldb order by rand() limit " + qtd)
   niddf.createOrReplaceTempView("niddf")
   
   val ncoldf = niddf.sqlContext.sql("select o.atid, case when o.atid == n.atid then null else o." + attribute + " end as ncolumn from originaldb o left join niddf n on o.atid = n.atid")
   ncoldf.createOrReplaceTempView("ncoldf")
   
   var rdf = niddf.sqlContext.sql("select o.*, n.ncolumn from originaldb o, ncoldf n where o.atid == n.atid")
   rdf = rdf.withColumn(attribute, rdf("ncolumn")).drop("ncolumn").drop("atid")
   
   return rdf
    
  }
  
}