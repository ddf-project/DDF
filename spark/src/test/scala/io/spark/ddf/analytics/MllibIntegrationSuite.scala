package io.spark.ddf.analytics

import scala.collection.JavaConversions._
import io.spark.ddf.{ATestSuite, SparkDDF}

/**
  */
class MLlibIntegrationSuite extends ATestSuite {

  test("test Kmeans") {

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val kmeansModel = ddfPredict2.ML.KMeans(5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")
    val yPred = ddfPredict2.ML.applyModel(kmeansModel, false, true)
    val nrows = yPred.VIEWS.head(10)
    assert(nrows != null)
  }

  test("test LinearRegressionWithSGD") {
    val trainDDF = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")
    val predictDDF = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")
    val regressionModel = trainDDF.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double)
    val yTrueYpred = predictDDF.ML.applyModel(regressionModel, true, true)

    val nrows = yTrueYpred.VIEWS.head(10)
    assert(nrows != null)
    println("YTrue YPred")
    for (x <- nrows) println(x)
  }

  test("test LogicticRegressionWithSGD") {
    // numIterations = 10, stepSize = 0.1
    val trainDDF = manager.sql2ddf("select " +
      "distance, depdelay, if (arrdelay > 10.89, 1, 0) as delayed from airline_delayed")
    val logRegModel = trainDDF.ML.train("logisticRegressionWithSGD", 10: java.lang.Integer, 0.1: java.lang.Double)
    val yPred = trainDDF.ML.applyModel(logRegModel, false, true)
    println(yPred.asInstanceOf[SparkDDF].getNumRows())
  }
}
