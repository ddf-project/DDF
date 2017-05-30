package io.ddf.spark.content

//import shark.api.Row
//import shark.memstore2.TablePartition

import java.util

import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.junit.Assert.assertEquals
import scala.collection.JavaConversions._
import io.ddf.spark.{ATestSuite, SparkDDF}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vectors, Vector}
import io.ddf.etl.IHandleMissingData.Axis

/**
  */
class RepresentationHandlerSuite extends ATestSuite {
  createTableAirline()

  test("Can get SchemaRDD and RDD[Vector]") {
    val ddf = manager.newDDF().getSqlHandler.sql2ddf("select month, year, dayofmonth from airline").asInstanceOf[SparkDDF]
    val rddVector = ddf.getRDD(classOf[Vector])
    assert(rddVector != null, "Can get RDD[Vector]")
    assert(rddVector.count == 295)

    val schemaRDD = ddf.getRepresentationHandler.get(classOf[DataFrame])
    assert(schemaRDD != null, "Can get SchemaRDD")
  }

  test("Can get RDD[LabeledPoint]") {
    manager.sql("drop table if exists airline_delayed", "SparkSQL")
    manager.sql("create table airline_delayed as SELECT *, if(abs(arrdelay)>10,1,0) as delayed FROM airline", "SparkSQL")
    val ddf = manager.sql2ddf("select " +
      "distance/1000, arrdelay/100, depdelay/100, delayed from airline_delayed", "SparkSQL").asInstanceOf[SparkDDF]
    val rddLabeledPoint = ddf.getRDD(classOf[LabeledPoint])
    assert(rddLabeledPoint != null)
    assert(rddLabeledPoint.count() === 301)

    val ddf2 = manager.sql2ddf("select month, year, dayofmonth from airline_delayed", "SparkSQL").asInstanceOf[SparkDDF]
    val rddLabeledPoint2 = ddf2.getRDD(classOf[LabeledPoint])
    assert(rddLabeledPoint2 != null)
    assert(rddLabeledPoint2.count() === 295)
  }

  test("Can get RDD[Array[Double]] and RDD[Array[Object]]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline", "SparkSQL").asInstanceOf[SparkDDF]
    val rddArrObj = ddf.getRDD(classOf[Array[Object]])

    val rddArrDouble = ddf.getRDD(classOf[Array[Double]])

    assert(rddArrDouble != null, "Can get RDD[Array[Double]]")
    assert(rddArrObj != null, "Can get RDD[Array[Object]]")
    assert(rddArrDouble.count() === 295)
    assert(rddArrObj.count() === 301)
  }

  test("Can get RDD[Array[Object]] & RDD[LabeledPoint] from RDD[Array[Double]]") {
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline", "SparkSQL").asInstanceOf[SparkDDF]
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
    val ddf = manager.sql2ddf("select month, year, dayofmonth from airline", "SparkSQL").asInstanceOf[SparkDDF]
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
    val ddf = manager.sql2ddf("select year, month, dayofmonth from airline", "SparkSQL").asInstanceOf[SparkDDF]

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
    val ddf = manager.sql2ddf("select * from airline", "SparkSQL").asInstanceOf[SparkDDF]
    for (split <- ddf.ML.CVKFold(5, 10)) {
      val train = split.getTrainSet.asInstanceOf[SparkDDF]
      val test = split.getTestSet.asInstanceOf[SparkDDF]
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

  test("Can get RDD(Array[String])") {
    createTableText8Sample 
    val ddf = manager.sql2ddf("select * from text8sample","SparkSQL")
    val doc = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Array[String]])
    val repHandler = ddf.getRepresentationHandler
    assert(doc != null)        
    assert(repHandler.has(classOf[RDD[_]], classOf[Array[String]]))
  }

  test("Make Vector") {
    val v = Vectors.sparse(10, Array(1, 5, 6), Array(2.0, 10.0, 5.0))
    val elements = Array(1.0, 2.0, 3.0, v)
    val vector = Row2LabeledPoint.makeVector(elements).get
    assert(vector.isInstanceOf[SparseVector])
    assert(vector.size == 13)
    assert(vector.apply(0) == 1.0)
    assert(vector.apply(1) == 2.0)
    assert(vector.apply(2) == 3.0)
    assert(vector.apply(3) == 0.0)
    assert(vector.apply(4) == 2.0)
    assert(vector.apply(5) == 0.0)
    assert(vector.apply(6) == 0.0)
    assert(vector.apply(7) == 0.0)
    assert(vector.apply(8) == 10.0)
    assert(vector.apply(9) == 5.0)
  }

  test("rowToArrayDouble") {
    val v = Vectors.sparse(10, Array(1, 5, 6), Array(2.0, 10.0, 5.0))
    val elements = Array(1.0, 2.0, 3, v)
    val row = Row.fromSeq(elements)
    val columns = Array(
      new Column("c1", ColumnType.DOUBLE),
      new Column("c2", ColumnType.DOUBLE),
      new Column("c3", ColumnType.INT),
      new Column("c4", ColumnType.VECTOR)
    )
    val arrDouble = RDDRow2ArrayDouble.rowToArrayDouble(row, columns)
    assert(arrDouble.apply(0) == 1.0)
    assert(arrDouble.apply(1) == 2.0)
    assert(arrDouble.apply(2) == 3)
    assert(arrDouble.apply(3) == 0.0)
    assert(arrDouble.apply(4) == 2.0)
    assert(arrDouble.apply(5) == 0.0)
    assert(arrDouble.apply(6) == 0.0)
    assert(arrDouble.apply(7) == 0.0)
    assert(arrDouble.apply(8) == 10.0)
    assert(arrDouble.apply(9) == 5.0)
  }

  test("creating LabeledPoint") {
    val airline = manager.sql2ddf("select * from airline", "SparkSQL")
    val transformedDF = airline.getTransformationHandler.factorIndexer(util.Arrays.asList("month"))
    val encodedDF = transformedDF.getTransformationHandler.oneHotEncoding("month", "vector").VIEWS.project(
      "year", "arrdelay", "vector", "depdelay")
    val rddLabeledPoint = encodedDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[LabeledPoint]).
      asInstanceOf[RDD[LabeledPoint]]
    val localArr = rddLabeledPoint.collect()
    localArr.foreach {
      row => assert(row.features.size == 12)
    }
  }

  test("creating Array[Double]") {
    val airline = manager.sql2ddf("select * from airline", "SparkSQL")
    val transformedDF = airline.getTransformationHandler.factorIndexer(util.Arrays.asList("month"))
    val encodedDF = transformedDF.getTransformationHandler.oneHotEncoding("month", "vector").VIEWS.project(
      "year", "arrdelay", "vector", "depdelay")
    val arrDouble = encodedDF.getRepresentationHandler.get(classOf[RDD[_]], classOf[Array[Double]]).
      asInstanceOf[RDD[Array[Double]]].collect()
    arrDouble.foreach {
      row => assert(row.size == 13)
    }
  }
}
