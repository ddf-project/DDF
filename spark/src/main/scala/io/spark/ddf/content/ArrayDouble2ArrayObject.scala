package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import io.ddf.content.{ Representation, ConvertFunction }
import io.ddf.DDF
import io.ddf.content.Schema.ColumnType
import scala.collection.JavaConversions._

/**
 */

class ArrayDouble2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

	override def apply(representation: Representation): Representation = {
		val mappers = ddf.getSchemaHandler.getColumns.map {
			column ⇒ this.getDouble2ObjectMapper(column.getType)
		}
		val rddArrObj = representation.getValue match {
			case rdd: RDD[Array[Double]] ⇒ rdd.map {
				row ⇒
					{
						var i = 0
						val arrObj = new Array[Object](row.size)
						while (i < row.size) {
							arrObj(i) = mappers(i)(row(i))
							i += 1
						}
						arrObj
					}
			}
		}
		new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
	}

	private def getDouble2ObjectMapper(colType: ColumnType): Double ⇒ Object = {
		colType match {
			case ColumnType.DOUBLE ⇒ {
				case double ⇒ double.asInstanceOf[Object]
			}
			case ColumnType.INT ⇒ {
				case double ⇒ double.toInt.asInstanceOf[Object]
			}
		}
	}
}
