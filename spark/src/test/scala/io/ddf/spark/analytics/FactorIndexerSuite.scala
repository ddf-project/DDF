package io.ddf.spark.analytics

import java.util

import io.ddf.{DDF, Factor}
import io.ddf.spark.ATestSuite

/**
 * Created by huandao on 3/15/16.
 */
class FactorIndexerSuite extends ATestSuite {

  createTableCarOwner()
  test("factor indexer") {
    val ddf = manager.sql2ddf("select * from carowner", "SparkSQL")
//    ddf.setAsFactor("name")
//    val transformedDDF = ddf.Transform.factorIndexer(Array("name"))
//    val factor = transformedDDF.getColumn("name").getOptionalFactor
//    println(s">>> clazz = " + factor.getParameterizedType.toGenericString)
    //val inversedTransformedDDF = transformedDDF.Transform.inverseFactorIndexer(Array("name"))
//    println(s">>> nrow = " + inversedTransformedDDF.getNumRows)
//    val data = inversedTransformedDDF.VIEWS.head(10)
//    import scala.collection.JavaConversions._
//    data.foreach {
//      row => println(s">>> row = $row")
//    }
    val factor = new DoubleFactor(ddf, "am")
    println(s">>> clazz = " + factor.getParameterizedType.getCanonicalName)
    println(s">>> clazz = " + factor.clazz.getName)
  }
  class DoubleFactor(ddf: DDF, colname: String) extends Factor[DDF](ddf, colname)
}
