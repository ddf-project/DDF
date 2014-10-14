package io.spark.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.sql.SchemaRDD
import io.ddf.exception.DDFException

/**
  */
class SchemaRDD2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case rdd: SchemaRDD => {
        val rddArrObj = rdd.map {
          row => row.asInstanceOf[Seq[Any]].toArray
        }
        new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[Array[Object]]")
    }
  }
}
