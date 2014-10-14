package io.spark.ddf.analytics

import scala.collection.JavaConversions._
import io.spark.ddf.{ATestSuite, SparkDDF}

/**
  */
class MLlibIntegrationSuite extends ATestSuite {

  test("Test MLLib integation") {

    createTableAirlineWithNA()
    createTableAirline()

    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")

    //for glm
    val ddfTrain3 = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed")

    val ddfPredict2 = manager.sql2ddf("select " +
      "distance, arrdelay, depdelay, delayed from airline_delayed")

    val ddfTrain4 = manager.sql2ddf("select " +
      "distance, depdelay, if (arrdelay > 10.89, 1, 0) as delayed from airline_delayed")

    val kmeansModel = ddfPredict2.ML.KMeans(5: java.lang.Integer, 5: java.lang.Integer, 10: java.lang.Integer, "random")

    //for regression, need to add ONE for bias-term
    val initialWeight = for {
      x <- 0 until (ddfTrain3.getNumColumns - 1)
    } yield (math.random)


    val regressionModel = ddfTrain3.ML.train("linearRegressionWithSGD", 10: java.lang.Integer,
      0.1: java.lang.Double, 0.1: java.lang.Double, initialWeight.toArray)
    val yTrueYpred = ddfPredict2.ML.applyModel(regressionModel, true, true)
    val yPred = ddfPredict2.ML.applyModel(kmeansModel, false, true)
    val nrows = yTrueYpred.VIEWS.head(10)
    println("YTrue YPred")
    for (x <- nrows) println(x)

    // numIterations = 10, stepSize = 0.1
    val logRegModel = ddfTrain4.ML.train("logisticRegressionWithSGD", 10: java.lang.Integer, 0.1: java.lang.Double)
    val yPred1 = ddfTrain4.ML.applyModel(logRegModel, false, true)

    yTrueYpred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count
    yPred.asInstanceOf[SparkDDF].getRDD(classOf[Array[Double]]).count

    println(yPred1.asInstanceOf[SparkDDF].getNumRows())

    manager.shutdown()
  }
}
