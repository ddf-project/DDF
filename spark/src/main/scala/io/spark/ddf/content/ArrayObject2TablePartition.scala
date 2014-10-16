package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF
import io.ddf.exception.DDFException

/**
  */
class ArrayObject2TablePartition(@transient ddf: DDF) extends ConvertFunction(ddf) with CanGetTablePartition {

  override def apply(representation: Representation): Representation = {
    val rddTP = representation.getValue match {
      case rdd: RDD[Array[Object]] ⇒ {
        val rddSeq = rdd.map {
          row ⇒ row.toSeq
        }.asInstanceOf[RDD[Seq[_]]]
        getRDDTablePartition(rddSeq, ddf.getSchemaHandler.getColumns, ddf.getTableName)
      }
      case _ ⇒ throw new DDFException("Error getting RDD[TablePartition]")
    }
    new Representation(rddTP, RepresentationHandler.RDD_TABLE_PARTITION.getTypeSpecsString)
  }
}
