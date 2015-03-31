package io.spark.ddf.content

import io.ddf.DDF
import org.apache.spark.sql.catalyst.expressions.Row
import io.ddf.content.Schema.DummyCoding
import io.ddf.types.TupleMatrixVector
import java.util.{List => JList}
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.exception.DDFException
import io.ddf.types.{Matrix, Vector}
import io.spark.ddf.ml.TransformRow
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.sql.SchemaRDD

/**
  */
class DataFrame2MatrixVector(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val columns = ddf.getSchemaHandler.getColumns
    val dummyCoding = ddf.getSchema.getDummyCoding
    val rddMatrixVector = representation.getValue match {
      case rdd: SchemaRDD => {
        rdd.mapPartitions {
          rows => rowsToMatrixVector(rows, columns, dummyCoding)
        }
      }
      case _ => throw new DDFException("Error getting RDD[(Matrix, Vector)]")
    }
    new Representation(rddMatrixVector, RepresentationHandler.RDD_MATRIX_VECTOR.getTypeSpecsString)
  }

  private def rowsToMatrixVector(rows: Iterator[Row], columns: JList[Column], dc: DummyCoding): Iterator[TupleMatrixVector] = {

    val listRows = rows.toList
    val numRows = listRows.size
    val numCols = columns.size
    val X = if (dc != null) {
      new Matrix(numRows, dc.getNumberFeatures())
    } else {
      new Matrix(numRows, numCols)
    }
    val Y = new Vector(numRows)
    val transformRow = new TransformRow(dc.xCols, dc.getMapping)

    println(">>>> numCols=" + numCols)
    println(">>>> numberOfFeatures = " + dc.getNumberFeatures)
    println(">>>> numRows = " + numRows)
    println(">>>> X.getNumRows = " + X.rows)
    println(">>>> X.getNumCols = " + X.columns)
    var rowIdx = 0
    val doubleExtractor = getDoubleExtractor(columns.toArray(new Array[Column](columns.size)))
    while (rowIdx < numRows) {
      var inputRow = listRows(rowIdx)
      X.put(rowIdx, 0, 1.0) // bias term
      var columnValue = 0.0
      var newValue: Double = -1.0
      val paddingBiasIndex = 1
      var columnIndex = 0
      var columnStringValue = ""
      while (columnIndex < numCols - 1) {
        if (transformRow.hasCategoricalColumn() && transformRow.hasCategoricalColumn(columnIndex)) {
          columnStringValue = inputRow(columnIndex).toString()
          newValue = dc.getMapping.get(columnIndex).get(columnStringValue)
        }
        else {
          //          println(">>> don't have dummyCoding")
          //          println(">>> column = " + columns(columnIndex).getName)
          newValue = doubleExtractor(columnIndex)(inputRow, columnIndex)
        }
        //        println(">>>> columnIndex = " + columnIndex)
        X.put(rowIdx, columnIndex + paddingBiasIndex, newValue)
        columnIndex += 1
      }
      Y.put(rowIdx, doubleExtractor(numCols - 1)(inputRow, columnIndex))
      rowIdx += 1
    }

    if (dc.getNumDummyCoding > 0) {
      //most important step
      var newX: Matrix = transformRow.instrument(X, dc.getxCols)
      //let's print the matrix
      val Z: TupleMatrixVector = new TupleMatrixVector(newX, Y)
      Iterator(Z)
    } else {
      val Z: TupleMatrixVector = new TupleMatrixVector(X, Y)
      Iterator(Z)
    }
  }

  def getDoubleExtractor(columns: Array[Column]): Array[(Row, Int) => Double] = {
    columns.map {
      col => col.getType match {
        case ColumnType.INT => {
          (row: Row, idx: Int) => row.getInt(idx).toDouble
        }
        case ColumnType.DOUBLE => {
          (row: Row, idx: Int) => row.getDouble(idx).toDouble
        }
        case ColumnType.FLOAT => {
          (row: Row, idx: Int) => row.getFloat(idx).toDouble
        }
        case ColumnType.LOGICAL => {
          (row: Row, idx: Int) => if (row.getBoolean(idx)) 1.0 else 0.0
        }
        case ColumnType.STRING => {
          (row: Row, idx: Int) => 0.0
        }
      }
    }
  }
}
