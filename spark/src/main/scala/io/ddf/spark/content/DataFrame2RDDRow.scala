package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD


/**
 */
class DataFrame2RDDRow(@transient ddf: DDF) extends ConvertFunction(ddf){

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: DataFrame => {
        val rddRow = rdd.rdd
        new Representation(rddRow, RepresentationHandler.RDD_ROW.getTypeSpecsString)
      }
    }
  }
}
