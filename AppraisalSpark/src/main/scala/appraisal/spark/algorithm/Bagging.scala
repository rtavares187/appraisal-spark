package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._
import appraisal.spark.util._

class Bagging(plan: ImputationPlan, T: Int) extends Serializable {
  
   var bdf = List.empty[(ImputationPlan)]
   var planName = ""
   
   var qT = (1 to T)
   
   if(plan.getParallel){
     
     qT.par.foreach(_T => {
     
       var nplan = new ImputationPlan(plan.getIdf.sample(true, 1.0), plan.getOdf, plan.getMissingRate, plan.getImputationFeature, plan.getFeatures)
       nplan.updateStrategies(plan.strategies)
       
       bdf = bdf :+ nplan
       planName = nplan.planName
     
     })
     
   }else{
   
     qT.foreach(_T => {
       
       var nplan = new ImputationPlan(plan.getIdf.sample(true, 1.0), plan.getOdf, plan.getMissingRate, plan.getImputationFeature, plan.getFeatures)
       nplan.updateStrategies(plan.strategies)
       
       bdf = bdf :+ nplan
       planName = nplan.planName
       
     })
     
   }
   
   def run() :Entities.ImputationResult = {
     
     var impRes = List.empty[Entities.ImputationResult]
     
     bdf.foreach(execB => impRes = impRes :+ execB.run())
     
     if(impRes.size > 1)
       Util.combineResult(impRes)
       
     else
       impRes.head
     
   }
  
}