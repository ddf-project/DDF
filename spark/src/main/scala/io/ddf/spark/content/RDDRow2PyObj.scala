package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.python.core.{PyDictionary, PyList}


/**
 * Convert a set of RDD rows into a dict of lists in python
 * Using "dict of list" here for efficiency
 *
 * Created by vupham on 7/27/15.
 */
class RDDRow2PyObj(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(rep: Representation): Representation = {
    val columnList = ddf.getSchemaHandler.getColumns

    rep.getValue match {
      case rdd: RDD[Row] =>
        val rddPyObj = rdd.mapPartitions {
          iterator => {
            val dctPartition = new PyDictionary()
            for (i <- 0 until columnList.size()) {
              dctPartition.put(columnList.get(i).getName, new PyList())
            }
            while (iterator.hasNext) {
              val row = iterator.next()
              for (i <- 0 until row.size) {
                dctPartition.get(columnList.get(i).getName).asInstanceOf[PyList].add(row.get(i))
              }
            }
            Iterator(dctPartition)
          }
        }
        new Representation(rddPyObj, RepresentationHandler.RDD_PYOBJ.getTypeSpecsString)
    }
  }
}
