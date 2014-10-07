package io.spark.ddf.util

import scala.collection.mutable.ArrayBuffer

object MLUtils {

  type PointType = (Array[Double], Int, Double)

  class filterFunction extends Function1[Array[Double], Boolean] with Serializable {
    override def apply(point: Array[Double]): Boolean =
      point != null
  }

  class DataPoint(val x: Array[Double], var numMembers: Int, var distance: Double) extends Serializable

  class ParsePoint(xCols: Array[Int], doCheckData: Boolean) extends Function1[Array[Object], Array[Double]] with Serializable {
    val base0_XCols: ArrayBuffer[Int] = new ArrayBuffer[Int]
    xCols.foreach(x ⇒ base0_XCols += x)

    override def apply(dataArray: Array[Object]): Array[Double] = {
      //var result: DataPoint = null
      var x = new ArrayBuffer[Double]
      if (dataArray.length > 0) {
        try {
          val dim: Int = base0_XCols.length
          if (doCheckData) {
            var i = 0
            while (i < dim) {
              dataArray(base0_XCols(i)) match {
                case i: java.lang.Integer ⇒ x += i.toDouble
                case d: java.lang.Double ⇒ x += d
                case _ ⇒ return null
              }
              i += 1
            }
          }
          else {
            //don't need to check data
            var i = 0
            while (i < dim) {
              dataArray(base0_XCols(i)) match {
                case i: java.lang.Integer ⇒ x += i.toDouble
                case d: java.lang.Double ⇒ x += d
              }
              i += 1
            }
          }

        }
        catch {
          case e: Exception ⇒ {

            throw new Exception(e)
          }
        }

      }
      x.toArray
    }
  }

}
