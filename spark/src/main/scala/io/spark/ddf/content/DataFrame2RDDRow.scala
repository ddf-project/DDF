package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.rdd.RDD

/**
 */
class DataFrame2RDDRow(@transient ddf: DDF) extends ConvertFunction(ddf){

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: SchemaRDD => {
        val rddRow = rdd.asInstanceOf[RDD[Row]]
        new Representation(rddRow, RepresentationHandler.RDD_ROW.getTypeSpecsString)
      }
    }
  }
}
