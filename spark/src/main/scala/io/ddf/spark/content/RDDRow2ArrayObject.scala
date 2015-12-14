package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import io.ddf.exception.DDFException

/**
  */
class RDDRow2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Row] => {
        val rddArrObj = rdd.map {
          row => row.toSeq.toArray
        }
        new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[Array[Object]]")
    }
  }
}
