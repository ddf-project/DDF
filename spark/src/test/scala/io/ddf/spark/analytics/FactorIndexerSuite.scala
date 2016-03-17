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

  test("factor indexer") {
    createTableFactor()
    val ddf = manager.sql2ddf("select * from factor", "SparkSQL")
    //ddf.setAsFactor("name")
    val originalData = ddf.VIEWS.head(20).asScala
    val transformedDDF = ddf.Transform.factorIndexer(Array("name"))
    val data = transformedDDF.VIEWS.head(20).asScala
    assert(data(0) == "0.0\t1.0")
    assert(data(1) == "0.0\t2.0")
    assert(data(2) == "0.0\t0.0")
    assert(data(3) == "0.0\t3.0")
    assert(data(4) == "1.0\t8.0")
    assert(data(5) == "1.0\t1.0")
    assert(data(9) == "3.0\t12.0")
    val inversedTransformedDDF = transformedDDF.Transform.inverseFactorIndexer(Array("name"))
    val inversedTransformData = inversedTransformedDDF.VIEWS.head(20).asScala
    assert(inversedTransformData(0) == "A\t1.0")
    assert(inversedTransformData(1) == "A\t2.0")
    assert(inversedTransformData(2) == "A\t0.0")
    assert(inversedTransformData(3) == "A\t3.0")
    assert(inversedTransformData(4) == "B\t8.0")
    assert(inversedTransformData(5) == "B\t1.0")
    assert(inversedTransformData(6) == "B\t1.0")
    assert(inversedTransformData(7) == "C\t0.0")
    assert(inversedTransformData(8) == "C\t10.0")
    assert(inversedTransformData(9) == "\t12.0")
  }
}
