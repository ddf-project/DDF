package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import shark.api.Row
import io.ddf.content.{ Representation, ConvertFunction }
import io.ddf.DDF

/**
 */
class Row2ArrayDouble(@transient ddf: DDF) extends ConvertFunction(ddf) with ObjectToDoubleMapper with RowToArray {

	override def apply(representation: Representation): Representation = {
		val rdd = representation.getValue.asInstanceOf[RDD[Row]]
		val mappers = getMapper(ddf.getSchemaHandler.getColumns)
		val numCols = mappers.length
		val rddArrDouble = rdd.map {
			row ⇒ rowToArray(row, mappers, numCols)
		}.filter(row ⇒ row != null)
		new Representation(rddArrDouble, RepresentationHandler.RDD_ARR_DOUBLE.getTypeSpecsString)
	}
}
