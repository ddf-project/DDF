package io.spark.ddf.content

import io.ddf.DDF
import org.apache.spark.rdd.RDD
import shark.api.Row
import io.ddf.content.Schema.DummyCoding
import io.ddf.types.TupleMatrixVector
import java.util.ArrayList
import scala.collection.JavaConversions._
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.exception.DDFException
import io.ddf.types.{Matrix, Vector}
import io.spark.ddf.ml.TransformRow

/**
  */
class Row2MatrixVector(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getNumColumns
    val rddMatrixVector = representation.getValue match {
      case rdd: RDD[Row] ⇒ {
        rdd.mapPartitions {
          rows ⇒ rowsToMatrixVector(rows, numCols)
        }
      }
      case _ ⇒ throw new DDFException("Error getting RDD[(Matrix, Vector)]")
    }
    new Representation(rddMatrixVector, RepresentationHandler.RDD_MATRIX_VECTOR.getTypeSpecsString)
  }

  private def rowsToMatrixVector(rows: Iterator[Row], numCols: Int): Iterator[TupleMatrixVector] = {
    //copy original data to arrayList
    var lstRows = new ArrayList[Array[Object]]()
    var row = 0
    while (rows.hasNext) {
      var currentRow = rows.next
      var column = 0
      var b = new Array[Object](numCols)
      while (column < numCols) {
        //        b(column) = mappers(column)(currentRow.getPrimitive(column))
        b(column) = currentRow.getPrimitive(column)
        column += 1
      }
      lstRows.add(row, b)
      row += 1
    }

    val numRows = lstRows.size
    var Y = new Vector(numRows)
    val X = new Matrix(numRows, numCols)

    row = 0
    val yCol = 0

    while (row < lstRows.size) {

      var inputRow = lstRows(row)
      X.put(row, 0, 1.0) // bias term
      var columnValue = 0.0
      var newValue: Double = -1.0
      val paddingBiasIndex = 1
      var columnIndex = 0

      var skipRowBecauseNullCell = false
      while (columnIndex < numCols - 1 && !skipRowBecauseNullCell) {
        if (inputRow(columnIndex) == null) skipRowBecauseNullCell = true;
        if (!skipRowBecauseNullCell) {
          var columnStringValue = ""
          newValue = objectToDouble(inputRow(columnIndex))
          X.put(row, columnIndex + paddingBiasIndex, newValue) // x-feature #i
          columnIndex += 1
          newValue = -1.0 // TODO: dirty and quick fix, need proper review
        } //TODO handle it more gracefully
        else {
          X.put(row, columnIndex + paddingBiasIndex, 0) // x-feature #i
        }
      }
      skipRowBecauseNullCell = skipRowBecauseNullCell || (inputRow(numCols - 1) == null)
      //TODO do this more gratefully
      if (inputRow(numCols - 1) == null) skipRowBecauseNullCell = true
      //TODO need to handle this as in na.action
      if (skipRowBecauseNullCell)
        Y.put(row, 0) // y-value
      else Y.put(row, inputRow(numCols - 1).toString().toDouble) // y-value

      row += 1
    }

    val Z: TupleMatrixVector = new TupleMatrixVector(X, Y)
    Iterator(Z)
  }

  def objectToDouble(o: Object): Double = o match {
    case i: java.lang.Integer ⇒ i.toDouble
    case f: java.lang.Float ⇒ f.toDouble
    case d: java.lang.Double ⇒ d
    case e ⇒ throw new RuntimeException("not a numeric Object " + (if (e != null) e.toString()))
  }
}
