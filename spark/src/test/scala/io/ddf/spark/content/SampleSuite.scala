package io.ddf.spark.content

import io.ddf.spark.ATestSuite
import scala.collection.JavaConversions._
/**
 */
class SampleSuite extends ATestSuite {
  createTableMtcars()
  test("test sample with numrows to return array") {
    val ddf = manager.sql2ddf("select * from mtcars")
    val sample = ddf.VIEWS.getRandomSample(10)

    assert(sample(0)(0).asInstanceOf[Double] != sample(1)(0).asInstanceOf[Double])
    assert(sample(1)(0).asInstanceOf[Double] != sample(2)(0).asInstanceOf[Double])
    assert(sample(2)(0).asInstanceOf[Double] != sample(3)(0).asInstanceOf[Double])
    assert(sample.length == 10)
  }

  test("test sample with percentage to return ddf") {
    val ddf = manager.sql2ddf("select * from mtcars")
    val sample = ddf.VIEWS.getRandomSample(0.5, false, 1)
    //sample.getSchema.getColumns.foreach(c => {println(c.getName + " - " + c.getType)})
    println("sample: ")
    sample.VIEWS.head(3).foreach(println)
  }

  test("test sample with percentage to return array with ill-legal parameter") {
    val ddf = manager.sql2ddf("select * from mtcars")
    val sample = ddf.VIEWS.getRandomSample(5.0, false, 1)
    //sample.getSchema.getColumns.foreach(c => {println(c.getName + " - " + c.getType)})
    println("sample: ")
    sample.VIEWS.head(3).foreach(println)
  }
}
