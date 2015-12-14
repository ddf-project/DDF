package io.ddf.spark.content

import io.ddf.spark.ATestSuite
import scala.collection.JavaConversions._
/**
 */
class SampleSuite extends ATestSuite {
  createTableMtcars()
  val ddf = manager.sql2ddf("select * from mtcars", "SparkSQL")
  test("test sample with numrows") {
    val sample = ddf.VIEWS.getRandomSample(10)
    assert(sample(0)(0).asInstanceOf[Double] != sample(1)(0).asInstanceOf[Double])
    assert(sample(1)(0).asInstanceOf[Double] != sample(2)(0).asInstanceOf[Double])
    assert(sample(2)(0).asInstanceOf[Double] != sample(3)(0).asInstanceOf[Double])
    assert(sample.length == 10)
  }

  test("test sample with percentage") {
    val sample = ddf.VIEWS.getRandomSample(0.5, false, 1)
    //sample.getSchema.getColumns.foreach(c => {println(c.getName + " - " + c.getType)})
    println("sample: ")
    sample.VIEWS.head(3).foreach(println)
  }

  test("test sample with percentage when percentage is invalid") {
    try {
      val sample = ddf.VIEWS.getRandomSample(5.0, false, 1)
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
    val sampleddf = ddf.VIEWS.getRandomSample(0.5, false, 1)

    factors.foreach{col => sampleddf.getSchemaHandler.getColumn(col).getOptionalFactor != null}
  }
}
