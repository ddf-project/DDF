package io.ddf.spark.content

import _root_.io.ddf.DDF
import _root_.io.ddf.content.{Schema, Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import org.python.core._

/**
 * Convert a dict of lists PyObject into array of objects
 *
 * Created by vupham on 7/27/15.
 */
class PyObj2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val columnList = ddf.getSchemaHandler.getColumns
    val rddArrObj = representation.getValue match {
      case rdd: RDD[PyObject] => PyObj2ArrayObject.RDataFrameToArrayObject(rdd, columnList)
    }
    new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
  }
}

object PyObj2ArrayObject {

  /**
   * Convert a dict of lists PyObject into array of objects
   */
  def RDataFrameToArrayObject(rdd: RDD[PyObject], columnList: java.util.List[Schema.Column]): RDD[Array[Object]] = {

    val rddarrobj = rdd.flatMap {
      partdf => {

        if (!partdf.isMappingType) {
          throw new IllegalArgumentException("A dict PyObject is expected")
        }

        val dct = partdf.asInstanceOf[PyDictionary]

        val keys = dct.keys()
        val cols = dct.size()
        val rows = (0 until cols).map(j => dct.get(keys.get(j)).asInstanceOf[PyList].size()).max

        if (cols != columnList.size()) {
          throw new IllegalArgumentException("Invalid columnList")
        }

        val jData = Array.ofDim[Object](rows, cols)

        (0 until cols).foreach {
          j ⇒
            val colData = dct.get(columnList.get(j).getName).asInstanceOf[PyList]
            colData.get(0) match {
              case v: java.lang.Double ⇒
                (0 until colData.size()).foreach {
                  i => {
                    val vv = colData.get(i).asInstanceOf[java.lang.Double]
                    if (vv.isNaN) {
                      jData(i)(j) = null
                    } else {
                      jData(i)(j) = vv
                    }
                  }
                }

              case v: java.lang.Integer =>
                (0 until colData.size()).foreach {
                  i => jData(i)(j) = colData.get(i)
                }

              case v: java.lang.String =>
                (0 until colData.size()).foreach {
                  i => jData(i)(j) = colData.get(i)
                }
            }
        }
        jData
      }
    }

    rddarrobj
  }
}


