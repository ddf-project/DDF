package io.spark.ddf.content

import shark.api.Row
import scala.reflect.ClassTag

/**
 */
trait RowToArray {

	def rowToArray[T: ClassTag](row: Row, mappers: Array[Object ⇒ Option[T]], numCols: Int): Array[T] = {
		var i = 0
		var isNULL = false
		val array = new Array[T](numCols)
		while ((i < numCols) && !isNULL) {
			mappers(i)(row.getPrimitive(i)) match {
				case Some(number) ⇒ array(i) = number
				case None ⇒ isNULL = true
			}
			i += 1
		}
		if (isNULL) null else array
	}
}
