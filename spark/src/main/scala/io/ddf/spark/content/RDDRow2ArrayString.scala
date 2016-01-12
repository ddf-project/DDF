package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RDDRow2ArrayString (@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: RDD[Row] =>
        val rddArrString = rdd.map {
          row => {
            row.getString(0).split("[ ,]")
          }
        }
        new Representation(rddArrString, RepresentationHandler.RDD_ARR_STRING.getTypeSpecsString)
    }
  }
}