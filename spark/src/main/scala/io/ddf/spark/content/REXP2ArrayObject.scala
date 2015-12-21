package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction, Schema}
import org.apache.spark.rdd.RDD
import org.rosuda.REngine.{REXPString, REXPInteger, REXPDouble, REXP}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList

/**
 */
class REXP2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val rddArrObj = representation.getValue match {
      case rdd: RDD[REXP] => {
        val rddArr = REXP2ArrayObject.RDataFrameToArrayObject(rdd, ddf.getSchema.getColumns.asScala.map(_.getType).toList)
        rddArr
      }
    }
    new Representation(rddArrObj, RepresentationHandler.RDD_ARR_OBJECT.getTypeSpecsString)
  }
}

object REXP2ArrayObject {
  /**
   * Convert a RDD of R data.frames into a RDD of Object[]
   */
  def RDataFrameToArrayObject(rdd: RDD[REXP], colTypes: List[Schema.ColumnType]): RDD[Array[Object]] = {

    val rddarrobj = rdd.flatMap {
      partdf ⇒
        val dflist = partdf.asList()
        val partitionSize = (0 until dflist.size()).map(j ⇒ dflist.at(j).length()).reduce {
          (x, y) ⇒ math.max(x, y)
        }

        //      mLog.info("partdf.len = " + partdf.length())
        //      mLog.info("partitionSize = " + partitionSize)

        // big allocation!
        val jdata = Array.ofDim[Object](partitionSize, dflist.size())

        // convert R column-oriented AtomicVector to row-oriented Object[]
        // TODO: would be nice to be able to create BigR DataFrame from columnar vectors
        (0 until dflist.size()).foreach {
          j ⇒
            val rcolvec = dflist.at(j)
            dflist.at(j) match {
              case v: REXPDouble ⇒ {
                val data = rcolvec.asDoubles() // no allocation
                var i = 0 // row idx
                while (i < partitionSize) {
                  if (REXPDouble.isNA(data(i)))
                    jdata(i)(j) = null
                  else {
                    if (colTypes(j) == Schema.ColumnType.BIGINT) {
                      jdata(i)(j) = data(i).toLong.asInstanceOf[Object]
                    } else {
                      jdata(i)(j) = data(i).asInstanceOf[Object]
                    }

                  }

                  i += 1
                }
              }
              case v: REXPInteger ⇒ {
                val data = rcolvec.asIntegers() // no allocation
                var i = 0 // row idx
                while (i < partitionSize) {
                  if (REXPInteger.isNA(data(i)))
                    jdata(i)(j) = null
                  else
                    jdata(i)(j) = data(i).asInstanceOf[Object]
                  i += 1
                }
              }
              case v: REXPString ⇒ {
                val data = rcolvec.asStrings() // no allocation
                var i = 0 // row idx
                while (i < partitionSize) {
                  jdata(i)(j) = data(i)
                  i += 1
                }
              }
              // TODO: case REXPLogical
            }
        }

        jdata
    }

    rddarrobj
  }
}
