package io.spark.ddf.analytics

import io.ddf.{DDF, DDFManager}
import io.ddf.ml.IModel
import io.spark.ddf.{ATestSuite, SparkDDF}

/**
  */
class MLSupporterSuite extends ATestSuite {
  createTableAirline()

  test("Test KMeans Prediction") {
    val ddf: DDF = manager.sql2ddf("select year, month, dayofmonth from airline")
    val k: Int = 5
    val numIterations: Int = 5
    val kmeansModel: IModel = ddf.ML.train("kmeans", k: java.lang.Integer, numIterations: java.lang.Integer)
    val pred: SparkDDF = ddf.ML.applyModel(kmeansModel, false, true).asInstanceOf[SparkDDF]

    val numrows = pred.getNumRows
    assert(numrows > 0)
    manager.shutdown
  }
}
