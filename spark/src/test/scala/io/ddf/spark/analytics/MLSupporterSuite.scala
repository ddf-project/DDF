package io.spark.ddf.analytics

import io.ddf.DDF
import io.ddf.ml.IModel
import io.spark.ddf.{ATestSuite, SparkDDF}

/**
  */
class MLSupporterSuite extends ATestSuite {
  createTableAirlineSmall()

  test("Test KMeans Prediction") {
    val ddf: DDF = manager.sql2ddf("select year, month, dayofmonth from airline")
    val k: Int = 5
    val numIterations: Int = 5
    val kmeansModel: IModel = ddf.ML.KMeans(5, 5, 2, "random")
    val pred: SparkDDF = ddf.ML.applyModel(kmeansModel, false, true).asInstanceOf[SparkDDF]

    val numrows = pred.getNumRows
    assert(numrows > 0)
    manager.shutdown
  }
}
