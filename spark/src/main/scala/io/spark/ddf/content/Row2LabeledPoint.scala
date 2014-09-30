package io.spark.ddf.content

import org.apache.spark.rdd.RDD
import shark.api.Row
import org.apache.spark.mllib.regression.LabeledPoint
import io.ddf.content.{ Representation, ConvertFunction }
import io.ddf.DDF

/**
 */
class Row2LabeledPoint(@transient ddf: DDF) extends ConvertFunction(ddf) with ObjectToDoubleMapper with RowToArray {

	override def apply(representation: Representation): Representation = {
		val mappers = getMapper(ddf.getSchemaHandler.getColumns)
		val numCols = mappers.size
		val rddLabeledPoint = representation.getValue.asInstanceOf[RDD[Row]].map(row ⇒ {
			val features = rowToArray(row, mappers, numCols - 1)
			val label = mappers(numCols - 1)(row.getPrimitive(numCols - 1))
			if (features == null || label == None) {
				null
			}
			else {
				new LabeledPoint(label.get, features)
			}
		}).filter(row ⇒ row != null)
		new Representation(rddLabeledPoint, RepresentationHandler.RDD_LABELED_POINT.getTypeSpecsString)
	}
}
