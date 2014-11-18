package io.ddf.types

import org.jblas.DoubleMatrix
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix
import no.uib.cipr.matrix.DenseVector
import no.uib.cipr.matrix.DenseMatrix
import io.ddf.misc.ALoggable

class MatrixSparse(numRows: Int, numCols: Int) extends ALoggable {

  var crs: FlexCompRowMatrix = new FlexCompRowMatrix(numRows, numCols)

  /*
	 * matrix Vector multiplication
	 * input: adatao vector, column vector
	 * output: DoubleMatrix
	 * TO DO: optimize crs.mult
	 */
  def mmul(other: Vector): DoubleMatrix = {

    //print dimension
    //check dimension
    if (numCols != other.getRows()) {
      mLog.error(">>>>>>>>>>> Wrong dimension: matrix: " + numRows + " * " + numCols + "\tvector size=" + other.data.size)
      sys.exit(1)
    }

    //convert to DenseMatrix, n rows, 1 column
    val denseMatrix: DenseMatrix = new DenseMatrix(new DenseVector(other.data))
    var retMatrix: DenseMatrix = new DenseMatrix(crs.numRows(), denseMatrix.numColumns())
    val startTime = System.currentTimeMillis()
    crs.mult(denseMatrix, retMatrix)
    val endTime = System.currentTimeMillis()
    MatrixSparse.toDoubleMatrix(retMatrix)

  }

  def print() {
    val str = "matrix: " + numRows + " * " + numCols + "\n " + this.crs.toString()
  }

  /*
	 * input: DoubleMatrix (implicitly vector)
	 * output: MatrixSparse
	 * assumption: dmatrix is dense matrix AND small enough to store in memory,  
	 * TO DO: optimize to copy block by block
	 * 
	 * time complexity is similar "upper bound" for CRS.mult(densematrix)
	 * CURRENT: not using
	 */
  def mmul_notused(dmatrix: DoubleMatrix): DenseMatrix = {

    //returned matrix
    //m * n  multiply n * p return m * p
    val retMatrix = new DenseMatrix(this.numRows, dmatrix.getColumns())

    val startTime = System.currentTimeMillis()

    //first: converted to dense matrix
    val convertedMatrix = new DenseMatrix(dmatrix.getRows(), dmatrix.getColumns())
    var rows = 0
    var columns = 0
    while (rows < dmatrix.getRows()) {
      columns = 0
      while (columns < dmatrix.getColumns()) {
        convertedMatrix.set(rows, columns, dmatrix.get(rows, columns))
        columns += 1
      }
      rows += 1
    }

    val endTime = System.currentTimeMillis()

    //second: multiply CRS sparse matrix with dense matrix 
    crs.mult(convertedMatrix, retMatrix)

    val endTime2 = System.currentTimeMillis()

    retMatrix
  }

  /*
	 * compute: dense matrix with CRS sparse matrix (this.crs)
	 * time complexity is similar "upper bound" by denseMatrix.multAdd api 
	 */
  def mmul2(dmatrix: DoubleMatrix): DenseMatrix = {

    val startTime = System.currentTimeMillis()

    //converted to dense matrix
    val convertedMatrix = new DenseMatrix(dmatrix.getRows(), dmatrix.getColumns())
    var rows = 0
    var columns = 0
    while (rows < dmatrix.getRows()) {
      columns = 0
      while (columns < dmatrix.getColumns()) {
        convertedMatrix.set(rows, columns, dmatrix.get(rows, columns))
        columns += 1
      }
      rows += 1
    }

    val endTime = System.currentTimeMillis()

    //returned matrix
    //m * n  multiply n * p return m * p
    val retMatrix = new DenseMatrix(convertedMatrix.numRows, crs.numColumns())

    //TO DO: multAdd is VERY slow 
    convertedMatrix.multAdd(crs, retMatrix)

    val endTime2 = System.currentTimeMillis()

    retMatrix
  }
}

object MatrixSparse {

  /*
	 * return column vecotr
	 */
  def toDoubleMatrix(inputVector: no.uib.cipr.matrix.Vector): DoubleMatrix = {
    val retMatrix = new DoubleMatrix(inputVector.size())
    var i = 0
    while (i < inputVector.size()) {
      retMatrix.put(i, 0, inputVector.get(i))
      i += 1
    }
    retMatrix
  }

  def toDoubleMatrix(inputMatrix: no.uib.cipr.matrix.Matrix): DoubleMatrix = {
    val retMatrix = new DoubleMatrix(inputMatrix.numRows(), inputMatrix.numColumns())
    var rows = 0
    var columns = 0
    while (rows < inputMatrix.numRows()) {
      columns = 0
      while (columns < inputMatrix.numColumns()) {
        retMatrix.put(rows, columns, inputMatrix.get(rows, columns))
        columns += 1
      }
      rows += 1
    }
    retMatrix
  }
}
