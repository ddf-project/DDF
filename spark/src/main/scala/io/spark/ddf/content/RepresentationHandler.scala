/**
 *
 */
package io.spark.ddf.content

import java.lang.Class
import scala.reflect.Manifest
import scala.collection.JavaConversions._
import io.spark.ddf.content.RepresentationHandler._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import io.ddf.content.{RepresentationHandler ⇒ RH, Representation}
import org.rosuda.REngine._
import io.ddf._
import io.ddf.types.TupleMatrixVector
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * RDD-based SparkRepresentationHandler
 *
 */

class RepresentationHandler(mDDF: DDF) extends RH(mDDF) {
  /**
   * Initialize RepresentationGraph
   */
  this.addConvertFunction(RDD_ARR_DOUBLE, RDD_ARR_OBJECT, new ArrayDouble2ArrayObject(this.mDDF))
  this.addConvertFunction(RDD_ARR_DOUBLE, RDD_LABELED_POINT, new ArrayDouble2LabeledPoint(this.mDDF))
  this.addConvertFunction(RDD_ARR_OBJECT, RDD_ARR_DOUBLE, new ArrayObject2ArrayDouble(this.mDDF))

  this.addConvertFunction(RDD_REXP, RDD_ARR_OBJECT, new REXP2ArrayObject(this.mDDF))
  this.addConvertFunction(RDD_ROW, RDD_ARR_OBJECT, new RDDRow2ArrayObject(this.mDDF))
  this.addConvertFunction(RDD_ROW, RDD_ARR_DOUBLE, new RDDRow2ArrayDouble(this.mDDF))
  this.addConvertFunction(RDD_ARR_DOUBLE, RDD_VECTOR, new ArrayDouble2Vector(this.mDDF))
  this.addConvertFunction(RDD_ARR_OBJECT, SCHEMARDD, new ArrayObject2SchemaRDD(this.mDDF))
  this.addConvertFunction(RDD_ROW, RDD_REXP, new RDDROW2REXP(this.mDDF))
  this.addConvertFunction(SCHEMARDD, RDD_MATRIX_VECTOR, new SchemaRDD2MatrixVector(this.mDDF))
  this.addConvertFunction(RDD_ROW, SCHEMARDD, new Row2SchemaRDD(this.mDDF))
  this.addConvertFunction(SCHEMARDD, RDD_ROW, new SchemaRDD2RDDRow(this.mDDF))
  //this.addConvertFunction(RDD_ROW, RDD_RATING, new Row2Rating(this.mDDF))

  override def getDefaultDataType: Array[Class[_]] = Array(classOf[RDD[_]], classOf[Array[Object]])

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   */
  def set[T](data: RDD[T])(implicit m: Manifest[T]) = {
    this.reset
    this.add(data)
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones
   */
  def add[T](data: RDD[T])(implicit m: Manifest[T]): Unit = this.add(data, classOf[RDD[_]], m.erasure)

  private def forAllReps[T](f: RDD[_] ⇒ Any) {
    mReps.foreach {
      kv ⇒ if (kv._2 != null) f(kv._2.asInstanceOf[RDD[_]])
    }
  }

  override def cacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          mLog.info(this.getClass() + ": Persisting " + rdd)
          rdd.persist
        }
    })
  }

  override def uncacheAll = {
    forAllReps({
      rdd: RDD[_] ⇒
        if (rdd != null) {
          mLog.info(this.getClass() + ": Unpersisting " + rdd)
          rdd.unpersist(false)
        }
    })
  }
}

object RepresentationHandler {

  /**
   * Supported Representations
   */
  val RDD_ARR_DOUBLE = new Representation(classOf[RDD[_]], classOf[Array[Double]])
  val RDD_ARR_OBJECT = new Representation(classOf[RDD[_]], classOf[Array[Object]])
  //  val RDD_ROW = new Representation(classOf[RDD[_]], classOf[Row])
  //  val RDD_TABLE_PARTITION = new Representation(classOf[RDD[_]], classOf[TablePartition])
  val RDD_LABELED_POINT = new Representation(classOf[RDD[_]], classOf[LabeledPoint])
  val RDD_MATRIX_VECTOR = new Representation(classOf[RDD[_]], classOf[TupleMatrixVector])
  val RDD_REXP = new Representation(classOf[RDD[_]], classOf[REXP])
  val SCHEMARDD = new Representation(classOf[SchemaRDD])
  val RDD_ROW = new Representation(classOf[RDD[_]], classOf[Row])
  val RDD_VECTOR = new Representation(classOf[RDD[_]], classOf[Vector])
  val RDD_RATING = new Representation(classOf[RDD[_]], classOf[Rating])
}
