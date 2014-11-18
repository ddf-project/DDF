/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.ddf.types

import org.jblas.DoubleMatrix
import com.google.gson.Gson
import scala.annotation.tailrec
import no.uib.cipr.matrix.sparse._

class Matrix(numRows: Int, numCols: Int) extends DoubleMatrix(numRows, numCols) with Iterable[DoubleMatrix] {

  /**
   * Instantiate given an Array[Array[Double]]. The length of the first row is assumed to be the maximum number of columns.
   */
  def this(doubleMatrix: Array[Array[Double]]) = {
    this(doubleMatrix.length, doubleMatrix(0).length)

    val numRows = doubleMatrix.length
    val maxCols = doubleMatrix(0).length

    var r = 0
    while (r < numRows) {
      val numCols = doubleMatrix(r).length
      var c = 0
      while (c < numCols && c < maxCols) {

        this.put(r, c, doubleMatrix(r)(c))

        //				crs.set(r, c, doubleMatrix(r)(c))
        c += 1
      }
      r += 1
    }

    //convert to crs
  }

  def crs: CompRowMatrix = new CompRowMatrix(this.numRows, this.numCols, null)

  /*
   * basically this experiment function is trying to avoid creating a copy of matrix Xt when doing XtX
   * in JBLAS it will create an additional Xt matrix instance along with X in order to compute XtX
   * this is somehow unnecessary because Xt is transpose of X and we don't need of copy for Xt
   *
   * XtX: Xt dimension is (columns, rows) and X dimension is (rows, columns) 
   * this is not in-place
   * the optimization happen by avoiding create additional matrix
   * also XtX is symmetric then we can compute the upper half and fill in the lower half later
   * XtX will be (columns, columns)  
   * time complexity: O(columns * columns/2 * rows) 
   * space complexity
   */
  def XtX(): Matrix = {
    var XtX = new Matrix(this.columns, this.columns)
    var i = 0

    var value = 0.0
    var t = 0
    var j = i
    while (i < this.columns) {
      j = i
      while (j < this.columns) {
        value = 0.0
        t = 0
        while (t < this.rows) {
          value += this.get(t, i) * this.get(t, j)
          t += 1
        }
        XtX.put(i, j, value)
        j += 1
      }
      i += 1
    }

    //fill in lower half of matrix
    i = 0
    j = 0
    while (i < XtX.rows) {
      j = 0
      while (j < i) {
        XtX.put(i, j, XtX.get(j, i))
        j += 1
      }
      i += 1
    }
    return XtX
  }

  private class MyIterator(matrix: Matrix) extends Iterator[DoubleMatrix] {
    private var cur = -1
    private val max = matrix.rows - 1

    def hasNext = (cur < max)

    def next: DoubleMatrix = {
      cur += 1
      matrix.getRow(cur)
    }
  }

  def iterator: Iterator[DoubleMatrix] = new MyIterator(this)

  def apply(row: Int, col: Int) = this.get(row, col)

  def apply(row: Int) = this.getRow(row)

  override def isEmpty = (this.length == 0)

  /*
     return a tuple(Array[Double], Array[Int])
     contain each row min and argmin of a 2D matrix
   */
  def rowMinAndArgsMin(): (Array[Double], Array[Int]) = {
    val min = Array.fill[Double](rows)(0)
    val argMin = Array.fill[Int](rows)(0)
    var r = 0
    while (r < rows) {
      val row = getRow(r)
      if (row.isEmpty) {
        min(r) = Double.PositiveInfinity
        argMin(r) = -1
      }
      else {
        var i = 0
        var v = Double.PositiveInfinity
        var index = -1
        while (i < row.length) {
          if (!row.get(i).isNaN && row.get(i) < v) {
            v = row.get(i)
            index = i
          }
          i += 1
        }
        min(r) = v
        argMin(r) = index
      }
      r += 1
    }
    (min, argMin)
  }

  /** Generate string representation of the matrix. */
  override def toString: String = {
    return toString("%f")
  }

  /**
   * Generate string representation of the matrix, with specified
   * format for the entries. For example, <code>x.toString("%.1f")</code>
   * generates a string representations having only one position after the
   * decimal point.
   */

  override def toString(fmt: String): String = {
    return toString(fmt, "[", "]", ", ", "\n ")
  }
}

object Matrix {
  def apply(arrayVector: Array[Vector]): Matrix = {
    var a = new DoubleMatrix(arrayVector.size, arrayVector(0).size)
    var i = 0
    arrayVector.foreach(x ⇒ {
      a.putRow(i, arrayVector(i))
      i += 1
    })
    Matrix(a)
  }

  def apply(dm: DoubleMatrix): Matrix = {
    val m = new Matrix(dm.rows, dm.columns)
    m.data = dm.data
    m
  }

  /**
   * Create a column vector
   */
  def ones(m: Int): Matrix = {
    val matrix: Matrix = new Matrix(m, 1)
    var r = 0
    while (r < m) {
      matrix.put(r, 0, 1)
      r += 1
    }
    matrix
  }
}

/**
 * Internally represented as a DoubleMatrix column vector.
 * Constructors are in the companion object, [[Vector]]
 */
class Vector(size: Int) extends DoubleMatrix(size) with Iterable[Double] with TJsonSerializable {
  //class Vector(size: Int) extends DoubleMatrix(size) with scala.Serializable with Iterable[Double] {

  /**
   * This empty constructor is needed for deserialization
   */
  def this() = this(0)

  private class MyIterator(vector: Vector) extends Iterator[Double] {
    private var cur = -1
    private val max = vector.rows - 1

    def hasNext = (cur < max)

    def next: Double = {
      cur += 1
      vector.get(cur, 0)
    }
  }

  def iterator: Iterator[Double] = new MyIterator(this)

  /**
   * Support for the idiom "vector(i)"
   */
  def apply(i: Int) = this.get(i)

  override def isEmpty = (this.length == 0)

  /**
   * Override to return a Vector instead of the underlying DoubleMatrix
   */
  override def dup = Vector(this, false)

  def columnSum: Double = this.columnSums.get(0, 0)

  /**
   * Performs dot product with 'other' vector, ignoring any product where our corresponding element is zero.
   * Thus, safely returns a result even when there are cases where we have 0 times infinite.
   *
   * NB: if *we* have an infinity while the other vector's corresponding element is zero, this would still
   * return NaN. We explicitly want this behavior to support only cases where the filtering is mathematically
   * specific to this vector, not the other. The goal isn't to avoid all NaN's, but to avoid NaN where we have
   * already excluded them out by mathematical definition. The main use case here is computing the loss function
   * in logistic regression.
   */
  def zeroFilteredDot(other: DoubleMatrix) = {
    var sum = 0.0
    var i = 0
    while (i < this.length) {
      if (this.get(i) != 0) sum += this.get(i) * other.get(i)
      i += 1
    }
    sum
  }

  /**
   * Override TJsonSerializable.toJson() to serialize only the Array[Double], since this is
   * the interface that clients who understand the TSerialiable interface expects.
   */
  override def toJson: String = TJsonSerializable.basicGson.toJson(this.data)

  /**
   * Override TJsonSerializable.toJson() to serialize only the Array[Double], since this is
   * the interface that clients who understand the TSerialiable interface expects.
   */
  //	override def fromJson(jsonString: String) = Vector(TJsonSerializable.basicGson.fromJson(jsonString, classOf[Array[Double]]))
  //	override def toString: String = ""

  def reciprocal: Vector = Vector(DoubleMatrix.ones(this.length).divi(this))

  override def toString: String = {
    return toString("%f")
  }
}

object Vector {
  //	def fromJson(jsonString: String): Vector = new Vector(0).fromJson(jsonString)

  /**
   * Instantiate a new Vector given the contents of a DoubleMatrix. For efficiency, by default
   * we do not copy the underlying array of doubles, but rather simply puts a pointer to it.
   *
   * @param dm - the source matrix which is treated as a 1-dimensional array of doubles (row-major).
   * @param doInPlace - if false, then perform a copy of the underlying data
   */
  def apply(v: Vector, doInPlace: Boolean = true): Vector = {
    val v2: Vector = new Vector(v.length)
    if (doInPlace) v2.data = v.data else v2.data = v.data.clone
    v2
  }

  /**
   * Instantiate a new Vector given the contents of a DoubleMatrix. For efficiency we do not
   * copy the underlying array of doubles, but rather simply puts a pointer to it.
   *
   * @param dm - the source matrix which is treated as a 1-dimensional array of doubles (row-major).
   */
  def apply(dm: DoubleMatrix): Vector = {
    val v = new Vector(dm.length)
    v.data = dm.data
    v
  }

  /**
   * Instantiate a new Vector given an Array of Doubles. For efficiency we do not
   * copy the underlying array of doubles, but rather simply puts a pointer to it.
   *
   * @param da - the source (1-dimensional) Array of Doubles
   */
  def apply(da: Array[Double]): Vector = {
    val v = new Vector(da.length)
    v.data = da
    v
  }

  /**
   * Instantiates a new Vector containing the given Doubles
   *
   * @param doubles - a varargs list of Doubles
   */
  def apply(doubles: Double*): Vector = {
    Vector(doubles.toArray[Double])
  }

  /**
   * Instantiates a new Vector of length len and fill it with values value
   */
  def fill(length: Int, value: Double) = {
    val result = new Vector(length)
    var i = 0
    while (i < length) {
      result.put(i, value)
      i += 1
    }
    result
  }
}

class TupleMatrixVector(val x: Matrix, val y: Vector) extends Tuple2[Matrix, Vector](x, y) {
}

