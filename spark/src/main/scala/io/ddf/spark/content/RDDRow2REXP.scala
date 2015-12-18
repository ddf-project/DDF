package io.ddf.spark.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.spark.rdd.RDD
import io.ddf.spark.{SparkDDFManager, SparkDDF}
import org.apache.spark.sql.Row
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.spark.sql.types.StructField
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.rosuda.REngine._

/**
 * // TODO review. What is this for?
  */
class RDDROW2REXP(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val columnList = ddf.getSchemaHandler.getColumns

    representation.getValue match {
      case rdd: RDD[Row] => {
        val rddREXP = rdd.mapPartitions {
          iterator => {
            val arrayBufferColumns: List[ArrayBuffer[_]] = columnList.map {
              col => col.getType match {
                case ColumnType.INT => new ArrayBuffer[Int]
                case ColumnType.DOUBLE | ColumnType.BIGINT => new ArrayBuffer[Double]
                case ColumnType.STRING => new ArrayBuffer[String]
              }
            }.toList

            while (iterator.hasNext) {
              val row = iterator.next()
              var i = 0
              while (i < row.size) {
                columnList(i).getType match {
                  case ColumnType.INT => {
                    val buffer = arrayBufferColumns(i).asInstanceOf[ArrayBuffer[Int]]
                    if (row.isNullAt(i)) {
                      buffer += REXPInteger.NA
                    } else {
                      buffer += row.getInt(i)
                    }
                  }

                  case ColumnType.BIGINT => {
                    val buffer = arrayBufferColumns(i).asInstanceOf[ArrayBuffer[Double]]
                    if (row.isNullAt(i)) {
                      buffer += REXPDouble.NA
                    } else {
                      buffer += row.getLong(i).toDouble
                    }
                  }

                  case ColumnType.DOUBLE | ColumnType.BIGINT => {
                    val buffer = arrayBufferColumns(i).asInstanceOf[ArrayBuffer[Double]]
                    if (row.isNullAt(i)) {
                      buffer += REXPDouble.NA
                    } else {
                      buffer += row.getDouble(i)
                    }
                  }

                  case ColumnType.STRING => {
                    val buffer = arrayBufferColumns(i).asInstanceOf[ArrayBuffer[String]]
                    if (row.isNullAt(i)) {
                      buffer += null
                    } else {
                      buffer += row.getString(i)
                    }
                  }
                }
                i += 1
              }
            }
            val rVectors = columnList zip arrayBufferColumns map {
              case (col, buffer) => col.getType match {
                case ColumnType.INT => {
                  new REXPInteger(buffer.asInstanceOf[ArrayBuffer[Int]].toArray).asInstanceOf[REXP]
                }
                case ColumnType.DOUBLE | ColumnType.BIGINT => {
                  new REXPDouble(buffer.asInstanceOf[ArrayBuffer[Double]].toArray).asInstanceOf[REXP]
                }
                case ColumnType.STRING => {
                  new REXPString(buffer.asInstanceOf[ArrayBuffer[String]].toArray).asInstanceOf[REXP]
                }
              }
            }
            val dfList = new RList(rVectors, columnList.map {
              col => col.getName
            })
            Iterator(REXP.createDataFrame(dfList))
          }
        }

        new Representation(rddREXP, RepresentationHandler.RDD_REXP.getTypeSpecsString)
      }
    }
  }
}
