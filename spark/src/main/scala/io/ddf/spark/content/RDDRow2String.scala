package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


/**
  * Convert a set of RDD rows into a set of strings
  *
  * Created by vupham on 7/27/15.
  */
class RDDRow2String(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case rdd: RDD[Row] =>
        new Representation(rdd.mapPartitions { iterator => iterator.map(r => r.toSeq.mkString(" ")) },
          RepresentationHandler.RDD_STRING.getTypeSpecsString)
    }
  }
}
