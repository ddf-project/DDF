/**
 *
 */
package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.IHandleViews
import scala.collection.JavaConverters._
import io.ddf.content.Schema
import io.spark.ddf.SparkDDF
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/**
 * RDD-based ViewHandler
 *
 *
 */
class ViewHandler(mDDF: DDF) extends io.ddf.content.ViewHandler(mDDF) with IHandleViews {

  object ViewFormat extends Enumeration {
    type ViewFormat = Value
    val DEFAULT, ARRAY_OBJECT, ARRAY_DOUBLE, TABLE_PARTITION, LABELED_POINT, LABELED_POINTS = Value
  }

  import ViewFormat._

  /**
   * Same as {@link #get(int[], int)}, but accepts a scala.Enumeration for format instead.
   *
   * @param columns
   * @param format
   * A scala.Enumeration that will be converted to an integer by calling
   * formatEnum.toString()
   * @return
   */
  def get(columns: Array[Int], format: ViewFormat): DDF = {
    format match {
      case ViewFormat.DEFAULT => ViewHandler.getDefault(columns, mDDF)
      case ViewFormat.ARRAY_OBJECT => ViewHandler.getArrayObject(columns, mDDF)
      case ViewFormat.ARRAY_DOUBLE => ViewHandler.getArrayDouble(columns, mDDF)
      case ViewFormat.TABLE_PARTITION => ViewHandler.getTablePartition(columns, mDDF)
      case ViewFormat.LABELED_POINT => ViewHandler.getLabeledPoint(columns, mDDF)
      case ViewFormat.LABELED_POINTS => ViewHandler.getLabeledPoints(columns, mDDF)
      case _ => {}
    }
    null
  }

  protected def getImpl(columns: Array[Int], format: String): DDF = {
    this.get(columns, ViewFormat.withName(format))
  }

  val MAX_SAMPLE_SIZE = 1000000;

  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): java.util.List[Array[Object]] = {

    if (numSamples > MAX_SAMPLE_SIZE) {
      throw new IllegalArgumentException("Number of samples is currently limited to %d".format(MAX_SAMPLE_SIZE))
    } else {
      if(mDDF.getRepresentationHandler.has(classOf[RDD[_]], classOf[Array[Object]])) {
        val rdd = mDDF.asInstanceOf[SparkDDF].getRDD(classOf[Array[Object]])
        val sampleData = rdd.takeSample(withReplacement, numSamples, seed).toList.asJava
        sampleData
      } else {
        val rdd = mDDF.asInstanceOf[SparkDDF].getRDD(classOf[Row])
        rdd.takeSample(withReplacement, numSamples, seed).map {
          row => {
            row.toSeq.toArray.asInstanceOf[Array[Object]]
          }
        }.toList.asJava
      }
    }
  }

  override def getRandomSample(percent: Double, withReplacement: Boolean, seed: Int): DDF = {
    val columns = mDDF.getSchema.getColumns
    val schema = new Schema(mDDF.getSchemaHandler.newTableName(), columns)
    val manager = this.getManager
    val sampleRdd = mDDF.asInstanceOf[SparkDDF].getRDD(classOf[Row]).sample(withReplacement, percent, seed)
    val sampleDDF = manager.newDDF(manager, sampleRdd, Array(classOf[RDD[_]], classOf[Row]), manager.getNamespace, null, schema)
    mLog.info(">>>>>>> adding ddf to DDFManager " + sampleDDF.getName)
    manager.addDDF(sampleDDF)
    sampleDDF.getMetaDataHandler.copyFactor(this.getDDF)
    sampleDDF
  }
}

object ViewHandler {
  def getDefault(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }

  def getArrayObject(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }

  def getArrayDouble(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }

  def getTablePartition(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }

  def getLabeledPoint(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }

  def getLabeledPoints(cols: Array[Int], theDDF: DDF): DDF = {

    null
  }
}
