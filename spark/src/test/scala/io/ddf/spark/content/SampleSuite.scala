package io.ddf.spark.content

import io.ddf.spark.ATestSuite
import scala.collection.JavaConversions._
/**
 */
class SampleSuite extends ATestSuite {
  createTableMtcars()
  val ddf = manager.sql2ddf("select * from mtcars", "SparkSQL")
  test("test sample with numrows") {
    val sample = ddf.VIEWS.sample(10, false, 1)
    assert(sample.getNumRows == 10)
    val sampleDF = sample.VIEWS.head(10)
    assert(sampleDF.get(0)(0).toString.toDouble != sampleDF.get(1)(0).toString.toDouble)
    assert(sampleDF.get(1)(0).toString.toDouble != sampleDF.get(2)(0).toString.toDouble)
    assert(sampleDF.get(2)(0).toString.toDouble != sampleDF.get(3)(0).toString.toDouble)
  }

  test("test sample with percentage") {
    val sample = ddf.VIEWS.sample(0.5, false, 1)
    //sample.getSchema.getColumns.foreach(c => {println(c.getName + " - " + c.getType)})
    println("sample: ")
    sample.VIEWS.head(3).foreach(println)
  }

  test("test sample with percentage when percentage is invalid") {
    try {
      val sample = ddf.VIEWS.sample(5.0, false, 1)
      //sample.getSchema.getColumns.foreach(c => {println(c.getName + " - " + c.getType)})
      println("sample: ")
      sample.VIEWS.head(3).foreach(println)
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }

  test("reserving factor column") {
    val factors = Array("cyl", "hp", "gear", "vs")
    factors.foreach{col => ddf.getSchemaHandler.setAsFactor(col)}
    val sampleDDF = ddf.VIEWS.sample(0.5, false, 1)

    factors.foreach{col => sampleDDF.getSchemaHandler.getColumn(col).getOptionalFactor != null}
  }
}
