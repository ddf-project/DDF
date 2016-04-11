package io.ddf.spark.content

import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.DDF
import org.apache.spark.sql.Row
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  */
class RDDRow2ArrayDouble(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val columns = ddf.getSchemaHandler.getColumns
    representation.getValue match {
      case rdd: RDD[Row] =>
        val rddArrDouble = rdd.map {
          row => RDDRow2ArrayDouble.rowToArrayDouble(row, columns.toArray(new Array[Column](columns.size())))
        }.filter(row => row != null)
        new Representation(rddArrDouble, RepresentationHandler.RDD_ARR_DOUBLE.getTypeSpecsString)
    }
  }
}

object RDDRow2ArrayDouble {

  def rowToArrayDouble(row: Row, columns: Array[Column]): Array[Double] = {
    var i = 0
    var isNull = false
    val values = mutable.ArrayBuilder.make[Double]
    while (i < columns.length && !isNull) {
      if (row.isNullAt(i)) {
        isNull = true
      } else {
        columns(i).getType match {
          case ColumnType.TINYINT => values += row.getByte(i).toDouble
          case ColumnType.SMALLINT => values += row.getShort(i).toDouble
          case ColumnType.INT => values += row.getInt(i).toDouble
          case ColumnType.BIGINT => values += row.getLong(i).toDouble
          case ColumnType.FLOAT => values += row.getFloat(i).toDouble
          case ColumnType.DOUBLE => values += row.getDouble(i)
          case ColumnType.BOOLEAN => row.getBoolean(i) match {
            case true => values += 1.0
            case false => values += 0.0
          }
          case ColumnType.VECTOR => {
            val vector = row.getAs[Vector](i)
            val arrDouble = vector.toArray
            values ++= arrDouble
          }
        }
      }
      i += 1
    }
    if (isNull) null else values.result()
  }
}
