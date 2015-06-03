package io.spark.ddf.content

import io.ddf.content.Schema.{ColumnType, Column}
import scala.collection.JavaConversions._
import io.ddf.exception.DDFException

/**
 *
 */
trait ObjectToDoubleMapper {

  def getMapper(columns: java.util.List[Column]): Array[Object => Option[Double]] = {
    columns.map(
      column => getDoubleMapper(column.getType)
    ).toArray
  }

  private def getDoubleMapper(colType: ColumnType): Object ⇒ Option[Double] = {
    colType match {
      case ColumnType.DOUBLE ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Double]) else None
      }

      case ColumnType.INT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Int].toDouble) else None
      }

      case ColumnType.STRING ⇒ {
        case _ ⇒ throw new DDFException("Cannot convert string to double")
      }

      case e ⇒ throw new DDFException("Cannot convert to double")
    }
  }
}
