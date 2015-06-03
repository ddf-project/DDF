package io.ddf.spark.content

import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF
import org.apache.spark.sql.catalyst.expressions.Row
import io.ddf.content.Schema.Column
import org.apache.spark.rdd.RDD

/**
  */
class RDDRow2ArrayDouble(@transient ddf: DDF) extends ConvertFunction(ddf) with RowToArray {

  override def apply(representation: Representation): Representation = {
    val columns = ddf.getSchemaHandler.getColumns
    representation.getValue match {
      case rdd: RDD[Row] =>
        val rddArrDouble = rdd.map {
          row => rowToArrayDouble(row, columns.toArray(new Array[Column](columns.size())))
        }.filter(row => row != null)
        new Representation(rddArrDouble, RepresentationHandler.RDD_ARR_DOUBLE.getTypeSpecsString)
    }
  }
}
