package io.ddf.spark.content

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import io.ddf.content.{Schema, Representation, ConvertFunction}
import io.ddf.DDF
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  */
class Row2LabeledPoint(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getSchemaHandler.getColumns.size
    val rddRow = representation.getValue.asInstanceOf[RDD[Row]]
    val doubleGetter = ObjectToDoubleMapper.getAny2DoubleMapper(ddf.getSchema.getColumns.last.getType)
    val rddLabeledPoint = rddRow.map {
      row => {
        val features = row.toSeq.take(numCols - 1)
        Row2LabeledPoint.makeVector(features) match {
          case Some(featureVector) =>
            doubleGetter(row.get(numCols - 1)) match {
              case Some(label) => new LabeledPoint(label, featureVector)
              case None => null
            }
          case None => null
        }
      }
    }.filter(row => row != null)

    new Representation(rddLabeledPoint, RepresentationHandler.RDD_LABELED_POINT.getTypeSpecsString)
  }

}

object Row2LabeledPoint {

  /**
    * Make a Vector out of a seq (normally a Row)
    *
    * @param elements sequence of elements
    * @return
    */
  def makeVector(elements: Seq[Any]): Option[Vector] = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    val iterator = elements.iterator
    var curr = 0
    var isNull = false
    while (iterator.hasNext) {
      iterator.next() match {
        case null => isNull = true
        case v: Vector =>
          v.foreachActive {
            case (idx, value) => if (value != 0.0) {
              indices += curr + idx
              values += value
            }
          }
          curr += v.size

        case d =>
          val value = d match {
            case d: Double => d
            case i: Int => i.toDouble
            case f: Float => f.toDouble
            case s: Short => s.toDouble
            case l: Long => l.toDouble
          }
          if (value != 0.0) {
            indices += curr
            values += value

            curr += 1
          }
      }
    }
    if (isNull) {
      None
    } else {
      Some(Vectors.sparse(curr, indices.result(), values.result()).compressed)
    }
  }

}
