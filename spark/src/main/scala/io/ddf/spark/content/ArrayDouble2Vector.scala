package io.spark.ddf.content

import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  */
class ArrayDouble2Vector(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Array[Double]] => {
        val rddVector: RDD[Vector] = rdd.map {
          row => Vectors.dense(row)
        }
        new Representation(rddVector, RepresentationHandler.RDD_VECTOR.getTypeSpecsString)
      }
    }
  }
}
