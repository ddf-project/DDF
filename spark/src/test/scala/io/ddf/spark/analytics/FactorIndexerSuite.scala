package io.ddf.spark.analytics

import java.util

import io.ddf.{DDF, Factor}
import io.ddf.spark.ATestSuite
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
 * Created by huandao on 3/15/16.
 */
class FactorIndexerSuite extends ATestSuite {

  createTableCarOwner()
  test("factor indexer") {
    val ddf = manager.sql2ddf("select * from carowner", "SparkSQL")
    ddf.setAsFactor("name")
    val transformedDDF = ddf.Transform.factorIndexer(Array("name"))
    val factor = transformedDDF.getColumn("name").getOptionalFactor
    val inversedTransformedDDF = transformedDDF.Transform.inverseFactorIndexer(Array("name"))
    assert(inversedTransformedDDF.getNumRows == 4)
    val transformedData = inversedTransformedDDF.VIEWS.head(10).asScala
    val originalData = ddf.VIEWS.head(10).asScala
    originalData.foreach {
      row => assert(transformedData.contains(row))
    }
  }
}
