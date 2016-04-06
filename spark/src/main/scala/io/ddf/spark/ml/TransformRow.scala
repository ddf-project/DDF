package io.ddf.spark.ml

import java.util.{Map => JMap}
import org.jblas.DoubleMatrix
import io.ddf.types.Matrix
import io.ddf.types.Vector

class TransformRow(xCols: Array[Int], mapping: JMap[java.lang.Integer, JMap[String, java.lang.Double]]) extends Serializable {

  var numNewColumns: Int = 0
  var iterator2 = mapping.keySet().iterator()
  while (iterator2.hasNext()) {
    numNewColumns += mapping.get(iterator2.next()).size() - 1
  }

  def hasCategoricalColumn(): Boolean = {
    return (mapping != null && mapping.size() > 0)
  }

  def hasCategoricalColumn(columnIndex: Int): Boolean = {
    (mapping != null && mapping.containsKey(columnIndex))
  }

  /*
	 * input column value "String"
	 * output: the mapping, double value
	 */
  def transform(columnIndex: Int, columnValue: String): Double = {
    if (mapping.containsKey(columnIndex)) {
      var v = mapping.get(columnIndex)
      if (v.containsKey(columnValue)) {
        v.get(columnValue)
      } else {
        -1.0
      }
    } else {
      -1.0
    }
  }

  /*
   * input rows of double
   * return rows off double with extra dummy columns
   *
   */
  def transform(row: Matrix): DoubleMatrix = {
    var oldNumColumns = row.data.length

    //new columns = bias term + old columns + new dummy columns 
    var newRow = new Vector(xCols.length + 1 + numNewColumns)

    //bias term
    var oldColumnIndex = 0
    var originalColumnValue = row.get(oldColumnIndex)
    var newColumnIndex = 0
    var originalColumnIndex = 0
    var bitmap = null.asInstanceOf[Vector]

    newRow.put(newColumnIndex, originalColumnValue)

    oldColumnIndex = 1
    newColumnIndex = 1

    var j = 0

    while (oldColumnIndex < oldNumColumns && oldColumnIndex <= xCols.length) {

      originalColumnValue = row.get(oldColumnIndex)
      originalColumnIndex = xCols(oldColumnIndex - 1)

      //if normal double column
      //xCols(oldColumnIndex-1) because of bias-term
      if (!mapping.containsKey(originalColumnIndex)) {
        newRow.put(newColumnIndex, originalColumnValue) // x-feature #i
        newColumnIndex += 1
      } else {
        //bitmap vector
        bitmap = getNewRowFromCategoricalRow(originalColumnValue, originalColumnIndex)

        j = 0
        while (j < bitmap.length) {
          newRow.put(newColumnIndex, bitmap(j))
          j += 1
          newColumnIndex += 1
        }
      }

      oldColumnIndex += 1
    }

    //convert to one vector
    new DoubleMatrix(newRow.data).transpose()
  }

  /*
	 * from double column value to dummy vector
	 * 		input: column value in double
	 * 		input: dummy column length, #number of dummy column
	 *   
	 * return bitmap vector with i th index wi ll be 1 if column value = i 
	 */
  def getNewRowFromCategoricalRow(columnValue: Double, originalColumnIndex: Int): Vector = {
    //k-1 level
    var dummyCodingLength = mapping.get(originalColumnIndex).size - 1
    var ret: Vector = new Vector(dummyCodingLength)
    var j = 0
    var colVal = columnValue.asInstanceOf[Int]
    while (j < dummyCodingLength) {
      if (colVal != 0) {
        if (j == colVal - 1) {
          ret.put(j, 1.0)
        } else {
          ret.put(j, 0.0)
        }
      } else {
        ret.put(j, 0.0)
      }
      j += 1
    }
    ret
  }

  def instrument[InputType](oldX: Matrix, xCols: Array[Int]): Matrix = {

    //so we need to do minus one for original column

    //add dummy columns
    val numCols = oldX.columns
    var numRows = oldX.rows

    //this is the most critical improvement to avoid OOM while building lm-categorical
    //basically we don't create a new matrix but rather updating value in-place

    //		val newX = new Matrix(numRows, numCols + numDummyCols)
    //		var newColumnMap = new Array[Int](numCols + numDummyCols)

    //new code, coulmnMap has same dimensions with input matrix columns
    var newColumnMap = new Array[Int](numCols)

    //row transformer
    //for each row
    var indexRow = 0
    var currentRow = null.asInstanceOf[Matrix]
    var newRowValues = null.asInstanceOf[DoubleMatrix]
    while (indexRow < oldX.rows) {

      //for each rows
      currentRow = Matrix(oldX.getRow(indexRow))

      newRowValues = this.transform(currentRow)
      //add new row
      oldX.putRow(indexRow, newRowValues)

      //convert oldX to new X
      indexRow += 1
    }
    oldX
  }
}
