package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Created by huandao on 5/26/15.
 */
class RDDInt2RDDRow(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val rddRow = representation.getValue match {
      case rdd: RDD[Int] => {
        rdd.map{i => Row(i)}
      }
    }
    new Representation(rddRow, RepresentationHandler.RDD_ROW.getTypeSpecsString)
  }
}
