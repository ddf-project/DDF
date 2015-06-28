package io.ddf.spark.content

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

  // TODO review @huan @freeman
  private def getDoubleMapper(colType: ColumnType): Object ⇒ Option[Double] = {
    colType match {
      case ColumnType.DOUBLE ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Double]) else None
      }
      case ColumnType.FLOAT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Float].toDouble) else None
      }

      case ColumnType.TINYINT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Byte].toDouble) else None
      }

      case ColumnType.SMALLINT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Short].toDouble) else None
      }

      case ColumnType.INT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Int].toDouble) else None
      }

      case ColumnType.BIGINT ⇒ {
        case obj ⇒ if (obj != null) Some(obj.asInstanceOf[Long].toDouble) else None
      }

      case ColumnType.STRING ⇒ {
        case _ ⇒ throw new DDFException("Cannot convert string to double")
      }

      case e ⇒ throw new DDFException("Cannot convert to double")
    }
  }
}
