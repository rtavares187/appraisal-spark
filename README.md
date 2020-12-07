# appraisal-spark

The volume of stored data and the demand for integration between the two continually grow.
This scenario increases the occurrence of a problem well known to the data scientists: the various
possibilities for inconsistencies. One of its common types, the absence of data, may impair the
analysis and outcome of any information-producing technique. Imputation is the area that studies
methods that seek to approximate the imputed value to the real one. The composite imputation
technique applies machine learning tasks in this process. It uses the imputation plan concept, a
logical sequence of strategies and algorithms used to produce the final imputed value. In this work,
the utilization of this technique is expanded, complementing its use with bagging, an ensemble
classifier. In this method, the data are divided into random groups and attached to classifiers
called base learners. For the generated subsets, the scores (percentage of assertiveness) of each
imputation plan are returned. The plan with greater assertiveness of all the subsets is indicated
as the suggestion of imputation for the complete set. The work is implemented in a framework
developed for the Apache Spark tool, called Appraisal-Spark, which aims to generate better values 
of predictability and performance for large-scale environments. Through it, it is possible to
compose several high performance imputation plans, evaluating strategies and comparing results.
Appraisal-Spark was used for imputation in two databases, with different characteristics and levels
of correlation between its attributes. Experiments in Appraisal-Spark are serially and parallelly
performed, exploring the use of the Apache Spark tool.

There are examples using several algorithms in multiple imputation plans described in the package: appraisal-spark/AppraisalSparkExecutor/src/main/scala/appraisal/spark/executor/poc/

Below is an example of how to use Appraisal Spark with different imputation plans using the Bagging technique in two databases:

    val kn = odf.count().intValue()
      
    var imputationPlans = List.empty[(String, Double, Double, Int, ImputationPlan)]

    val missingRate = Seq(10d, 20d, 30d)
    //val missingRate = Seq(10d)

    val selectionReduction = Seq(10d, 20d, 30d)
    //val selectionReduction = Seq(10d)

    val bT = Seq(1,2,3)
    //val bT = Seq(1)

    features.foreach(feat => {

      feature = feat
      odf = odf.withColumn("originalValue", col(feature))

      missingRate.foreach(mr => {

        //val idf = new Eraser().run(odf, feature, mr)
        val idf = null

        selectionReduction.foreach(sr => {

          bT.foreach(T => {

            // selection -> clustering -> regression

          var impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
          var impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

          var selectionParamsAvg: HashMap[String, Any] = HashMap(
          "percentReduction" -> sr)

          var selectionParamsKnn: HashMap[String, Any] = HashMap(
          "percentReduction" -> sr)

          impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
          impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))

          var clusteringParamsAvg: HashMap[String, Any] = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          var clusteringParamsKnn: HashMap[String, Any] = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
          impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))

          var imputationParamsAvg: HashMap[String, Any] = HashMap()

          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))

          var baggingParamsAvg: HashMap[String, Any] = HashMap(
          "imputationPlan" -> impPlanAvg,
          "T" -> T)

          impPlanAvg.addEnsembleStrategy(new EnsembleStrategy(baggingParamsAvg, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanAvg)

          var imputationParamsKnn: HashMap[String, Any] = HashMap(
          "k" -> 2,
          "kLimit" -> kn)

          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))

          var baggingParamsKnn: HashMap[String, Any] = HashMap(
          "imputationPlan" -> impPlanKnn,
          "T" -> T)

          impPlanKnn.addEnsembleStrategy(new EnsembleStrategy(baggingParamsKnn, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanKnn)



          // clustering -> selection -> regression

          impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
          impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

          clusteringParamsAvg = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          clusteringParamsKnn = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
          impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))

          selectionParamsAvg = HashMap(
          "percentReduction" -> sr)

          selectionParamsKnn = HashMap(
          "percentReduction" -> sr)

          impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
          impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))

          imputationParamsAvg = HashMap()

          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))

          //imputationPlans = imputationPlans :+ impPlanAvg

          imputationParamsKnn = HashMap(
          "k" -> 2,
          "kLimit" -> kn)

          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))

          baggingParamsKnn = HashMap(
          "imputationPlan" -> impPlanKnn,
          "T" -> T)

          impPlanKnn.addEnsembleStrategy(new EnsembleStrategy(baggingParamsKnn, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanKnn)



          // clustering -> regression

          impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
          impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

          clusteringParamsAvg = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          clusteringParamsKnn = HashMap(
          "k" -> 2, 
          "maxIter" -> 1000, 
          "kLimit" -> kn)

          impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
          impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))

          imputationParamsAvg = HashMap()

          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))

          baggingParamsAvg = HashMap(
          "imputationPlan" -> impPlanAvg,
          "T" -> T)

          impPlanAvg.addEnsembleStrategy(new EnsembleStrategy(baggingParamsAvg, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanAvg)

          imputationParamsKnn = HashMap(
          "k" -> 2,
          "kLimit" -> kn)

          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))

          baggingParamsKnn = HashMap(
          "imputationPlan" -> impPlanKnn,
          "T" -> T)

          impPlanKnn.addEnsembleStrategy(new EnsembleStrategy(baggingParamsKnn, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanKnn)



          // selection -> regression

          impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
          impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

          selectionParamsAvg = HashMap(
          "percentReduction" -> sr)

          selectionParamsKnn = HashMap(
          "percentReduction" -> sr)

          impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
          impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))

          imputationParamsAvg = HashMap()

          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))

          //imputationPlans = imputationPlans :+ impPlanAvg

          imputationParamsKnn = HashMap(
          "k" -> 2,
          "kLimit" -> kn)

          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))

          baggingParamsKnn = HashMap(
          "imputationPlan" -> impPlanKnn,
          "T" -> T)

          impPlanKnn.addEnsembleStrategy(new EnsembleStrategy(baggingParamsKnn, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanKnn)



          // regression

          impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
          impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

          imputationParamsAvg = HashMap()

          impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))

          baggingParamsAvg = HashMap(
          "imputationPlan" -> impPlanAvg,
          "T" -> T)

          impPlanAvg.addEnsembleStrategy(new EnsembleStrategy(baggingParamsAvg, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanAvg)

          imputationParamsKnn = HashMap(
          "k" -> 2,
          "kLimit" -> kn)

          impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))

          baggingParamsKnn = HashMap(
          "imputationPlan" -> impPlanKnn,
          "T" -> T)

          impPlanKnn.addEnsembleStrategy(new EnsembleStrategy(baggingParamsKnn, new Bagging()))

          imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlanKnn)

          })

        })

      })

    })

    //val imputationPlansRdd = spark.sparkContext.parallelize(imputationPlans)
    //imputationPlansRdd.map(plan => (plan.planName, plan.run())).foreach(println(_))

    var resultList = new Crowner().runEnsemle(imputationPlans, parallelExecution)

    var consResult = new Reviewer().runEnsemble(resultList, missingRate, selectionReduction)

    Logger.getLogger(getClass.getName).error("")
    Logger.getLogger(getClass.getName).error("")
    Logger.getLogger(getClass.getName).error("")
    Logger.getLogger(getClass.getName).error(" ------ CONSOLIDATED RESULT ------ ")

    consResult.foreach(x => {

      Logger.getLogger(getClass.getName).error("Plan: " + x._1 + "	" + "Missing rate: " + x._2 + "	" 
                                              + "Selection reduction: " + x._3 + "	" + "Error: " + x._4 + "%") 

    })

    val bestPlan = consResult.sortBy(_._4).head

    Logger.getLogger(getClass.getName).error("")
    Logger.getLogger(getClass.getName).error("Best plan: " + bestPlan._1 + "	" + "Missing rate: " + bestPlan._2 + "	" 
                                              + "Selection reduction: " + bestPlan._3 + "	" + "Error: " + bestPlan._4 + "%")

    val wallStopTime = new java.util.Date()

    val wallTimeseconds   = ((wallStopTime.getTime - wallStartTime.getTime) / 1000)

    val wallTimesMinutes = wallTimeseconds / 60

    val wallTimesHours = wallTimesMinutes / 60

    Logger.getLogger(getClass.getName).error("------------------ Wall stop time: "
                                            + appraisal.spark.util.Util.getCurrentTime(wallStartTime)
                                            + " --- Total wall time: " + wallTimeseconds + " seconds, "
                                            + wallTimesMinutes + " minutes, "
                                            + wallTimesHours + " hours.")
