package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import shark.api.Row
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF

/**
  */
class Row2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val rdd = representation.getValue.asInstanceOf[RDD[Row]]
    val rddArrObj = rdd.map {
      row ⇒ {
        val size = row.rawdata.asInstanceOf[Array[Object]].size
        val array = new Array[Object](size)
        var idx = 0
        while (idx < size) {
          array(idx) = row.getPrimitive(idx)
          idx += 1
        }
        array
      }
    }
    new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
  }
}
