package io.spark.ddf.content

//import shark.api.Row
//import shark.memstore2.TablePartition

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.junit.Assert.assertEquals
import scala.collection.JavaConversions._
import io.spark.ddf.{ATestSuite, SparkDDF}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame}
import org.rosuda.REngine.REXP
import io.ddf.etl.IHandleMissingData.Axis

/**
  */
class RepresentationHandlerSuite extends ATestSuite {
  createTableAirline()

  test("Can get SchemaRDD and RDD[Vector]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val rddVector = ddf.getRDD(classOf[Vector])
    assert(rddVector != null, "Can get RDD[Vector]")
    assert(rddVector.count == 295)

    val schemaRDD = ddf.getRepresentationHandler.get(classOf[DataFrame])
    assert(schemaRDD != null, "Can get SchemaRDD")
  }

  test("Can get RDD[LabeledPoint]") {
    manager.sql2txt("drop table if exists airline_delayed")
    manager.sql2txt("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline")
    val ddf = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed").asInstanceOf[SparkDDF]
    val rddLabeledPoint = ddf.getRDD(classOf[LabeledPoint])
    assert(rddLabeledPoint != null)
    assert(rddLabeledPoint.count() === 301)

    val ddf2 = manager.sql2ddf("select month, year, dayofmonth from airline_delayed").asInstanceOf[SparkDDF]
    val rddLabeledPoint2 = ddf2.getRDD(classOf[LabeledPoint])
    assert(rddLabeledPoint2 != null)
    assert(rddLabeledPoint2.count() === 295)
  }

  test("Can get RDD[Array[Double]] and RDD[Array[Object]]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val rddArrObj = ddf.getRDD(classOf[Array[Object]])

    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])

    assert(rddArrDouble != null, "Can get RDD[Array[Double]]")
    assert(rddArrObj != null, "Can get RDD[Array[Object]]")
    assert(rddArrDouble.count() === 295)
    assert(rddArrObj.count() === 301)
  }

  test("Can get RDD[Array[Object]] & RDD[LabeledPoint] from RDD[Array[Double]]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val repHandler = ddf.getRepresentationHandler
    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    repHandler.remove(classOf[RDD[_]], classOf[Row])
    val keys = ddf.getRepresentationHandler.getAllRepresentations.keySet()
    LOG.info(">>>> keys = " + keys.mkString(", "))
    val arrObj = ddf.getRDD(classOf[Array[Object]])
    assert(arrObj.count == 295)
    assert(arrObj != null)

    repHandler.remove(classOf[RDD[_]], classOf[Array[Object]])
    val arrLP = ddf.getRDD(classOf[LabeledPoint])
    assert(arrLP != null)
    assert(arrLP.count == 295)
  }

  test("Has representation after creating it") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val repHandler = ddf.getRepresentationHandler
    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    val rddArrObj = ddf.getRDD(classOf[Array[Object]])
    val rddArrLP = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[LabeledPoint])

    assert(rddArrDouble != null)
    assert(rddArrObj != null)
    assert(rddArrLP != null)

    assert(repHandler.has(classOf[RDD[_]], classOf[Array[Double]]))
    assert(repHandler.has(classOf[RDD[_]], classOf[Array[Object]]))
    assert(repHandler.has(classOf[RDD[_]], classOf[LabeledPoint]))
  }

  test("Can handle null value") {
    val ddf = manager.sql2ddf("select year, month, dayofmonth from airline").asInstanceOf[SparkDDF]

    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])
    val rddArrLP = ddf.getRDD(classOf[LabeledPoint])

    val ArrArrDouble = rddArrDouble.collect()

    ArrArrDouble.foreach {
      row => assert(row(0) != 0.0, "row(0) == %s, expecting not 0.0".format(row(0)))
    }
    val count = rddArrLP.count()

    assertEquals(295, ArrArrDouble.size)
    assertEquals(295, count)
  }

  test("Can do sql queries after CrossValidation ") {
    val ddf = manager.sql2ddf("select * from airline").asInstanceOf[SparkDDF]
    for (split <- ddf.ML.CVKFold(5, 10)) {
      val train = split(0).asInstanceOf[SparkDDF]
      val test = split(1).asInstanceOf[SparkDDF]
      val ddf1 = train.sql2ddf("select month, year, dayofmonth from @this")
      val ddf2 = test.sql2ddf("select * from @this")

      assert(ddf1 != null)
      assert(ddf2 != null)
      assert(ddf1.getNumColumns == 3)
      assert(ddf1.getNumRows + ddf2.getNumRows == 301)
    }
  }

  test("Handle empty DDF") {
    val ddf = manager.newDDF();
    val rdd = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row])
    assert(rdd == null)
  }

  test("Can do sql queries after Transform Rserve") {
    createTableMtcars()
    val ddf = manager.sql2ddf("select * from mtcars")
    val newDDF = ddf.Transform.transformNativeRserve("z1 = mpg / cyl, " +
      "z2 = disp * 0.4251437075, " +
      "z3 = rpois(nrow(df.partition), 1000)")
    val rddREXP = newDDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[REXP]).asInstanceOf[RDD[REXP]]
    assert(newDDF != null)
    val st = newDDF.VIEWS.head(32)
    val ddf1 = newDDF.sql2ddf("select * from @this")

    assert(ddf1.getNumRows == 32)
    assert(ddf1 != null)
  }
}
