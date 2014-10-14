//package io.spark.ddf.content
//
//import io.ddf.DDF
//import org.apache.spark.rdd.RDD
//import io.ddf.exception.DDFException
//import shark.memstore2.TablePartition
//import io.ddf.content.Schema.ColumnType
//import scala.collection.mutable
//import org.apache.hadoop.io.{Text, IntWritable}
//import org.rosuda.REngine._
//import org.apache.hadoop.hive.serde2.io.DoubleWritable
//import scala.collection.JavaConversions._
//import io.ddf.content.{Representation, ConvertFunction}
//
///**
//  */
//class TablePartition2REXP(@transient ddf: DDF) extends ConvertFunction(ddf) {
//
//  override def apply(representation: Representation): Representation = {
//    val columnList = ddf.getSchemaHandler.getColumns
//    representation.getValue match {
//      case rdd: RDD[TablePartition] => {
//        val rddREXP = rdd.filter {
//          tp ⇒ tp.iterator.columnIterators.length > 0
//        }.map {
//          tp ⇒
//            //        mLog.info("columnIterators.length = " + tp.iterator.columnIterators.length)
//            //        mLog.info("tp.numRows = " + tp.numRows)
//            // each TablePartition should not have more than MAX_INT rows,
//            // ArrayBuffer doesn't allow more than that anyway
//            val numRows = tp.numRows.asInstanceOf[Int]
//
//            val columns = columnList.zipWithIndex.map {
//              case (colMeta, colNo) ⇒
//                //mLog.info("processing column: {}, index = {}", colMeta, colNo)
//                val iter = tp.iterator.columnIterators(colNo)
//                val rvec = colMeta.getType match {
//                  case ColumnType.INT ⇒ {
//                    val builder = new mutable.ArrayBuilder.ofInt
//                    var i = 0
//                    while (i < tp.numRows) {
//                      iter.next()
//                      if (iter.current != null)
//                        builder += iter.current.asInstanceOf[IntWritable].get
//                      else
//                        builder += REXPInteger.NA
//                      i += 1
//                    }
//                    new REXPInteger(builder.result)
//                  }
//                  case ColumnType.DOUBLE ⇒ {
//                    val builder = new mutable.ArrayBuilder.ofDouble
//                    var i = 0
//                    while (i < tp.numRows) {
//                      iter.next()
//                      if (iter.current != null)
//                        builder += iter.current.asInstanceOf[DoubleWritable].get
//                      else
//                        builder += REXPDouble.NA
//                      i += 1
//                    }
//                    new REXPDouble(builder.result)
//                  }
//                  case ColumnType.STRING ⇒ {
//                    val buffer = new mutable.ArrayBuffer[String](numRows)
//                    var i = 0
//                    while (i < tp.numRows) {
//                      iter.next()
//                      if (iter.current != null)
//                        buffer += iter.current.asInstanceOf[Text].toString
//                      else
//                        buffer += null
//                      i += 1
//                    }
//                    new REXPString(buffer.toArray)
//                  }
//                  // TODO: REXPLogical
//                  case _ ⇒ throw new DDFException("Cannot convert to double")
//                }
//                rvec.asInstanceOf[REXP]
//            }
//
//            // named list of columns with colnames
//            val dflist = new RList(columns, columnList.map {
//              m ⇒ m.getName
//            })
//
//            // this is the per-partition Renjin data.frame
//            REXP.createDataFrame(dflist)
//        }
//        new Representation(rddREXP, RepresentationHandler.RDD_REXP.getTypeSpecsString)
//      }
//      case _ => throw new DDFException("Error getting RDD[REXP]")
//    }
//  }
//}
