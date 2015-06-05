package io.ddf.spark.content

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF
import io.ddf.exception.DDFException

/**
  */
class ArrayDouble2LabeledPoint(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getNumColumns
    representation.getValue match {
      case rdd: RDD[Array[Double]] => {
        val rddLabeledPoint = rdd.map {
          row => {
            val label = row(numCols - 1)
            val features = new Array[Double](numCols - 1)
            var i = 0
            while (i < numCols - 1) {
              features(i) = row(i)
              i += 1
            }
            new LabeledPoint(label, Vectors.dense(features))
          }
        }
        new Representation(rddLabeledPoint, RepresentationHandler.RDD_LABELED_POINT.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[LabeledPoint]")
    }
  }
}
