package io.ddf.spark.content

import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.sql.Row

//
///**
//  */
//trait RowToArray {
//
//  def rowToArray[T: ClassTag](row: Row, mappers: Array[Object ⇒ Option[T]], numCols: Int): Array[T] = {
//    var i = 0
//    var isNULL = false
//    val array = new Array[T](numCols)
//    while ((i < numCols) && !isNULL) {
//      mappers(i)(row.getPrimitive(i)) match {
//        case Some(number) => array(i) = number
//        case None => isNULL = true
//      }
//      i += 1
//    }
//    if (isNULL) null else array
//  }
//}

// TODO review @huan
trait RowToArray {
  def rowToArrayDouble(row: Row, columns: Array[Column]): Array[Double] = {
    var i = 0
    var isNull = false
    val array = new Array[Double](columns.length)
    while (i < columns.length && !isNull) {
      if (row.isNullAt(i)) {
        isNull = true
      } else {
        array(i) = columns(i).getType match {
          case ColumnType.TINYINT => row.getByte(i).toDouble
          case ColumnType.SMALLINT => row.getShort(i).toDouble
          case ColumnType.INT => row.getInt(i).toDouble
          case ColumnType.BIGINT => row.getLong(i).toDouble
          case ColumnType.FLOAT => row.getFloat(i).toDouble
          case ColumnType.DOUBLE => row.getDouble(i)
          case ColumnType.BOOLEAN => row.getBoolean(i) match {
            case true => 1.0
            case false => 0.0
          }
        }
      }
      i += 1
    }
    if (isNull) null else array
  }
}
