package io.spark.ddf.content

import shark.memstore2.TablePartition
import org.apache.spark.rdd.RDD
import java.util.{ Map ⇒ JMap }
import java.util.{ HashMap ⇒ JHMap }
import java.util.{ List ⇒ JList }
import java.lang.{ Integer ⇒ JInt }
import org.apache.hadoop.io.{ Text, FloatWritable, IntWritable }
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import io.ddf.content.Schema.ColumnType
import io.ddf.exception.DDFException
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
 */
object GetMultiFactor {

	//For Java interoperability
	def getFactorCounts[T](rdd: RDD[T], columnIndexes: JList[JInt], columnTypes: JList[ColumnType], rddUnit: Class[T]): JMap[JInt, JMap[String, JInt]] = {

		getFactorCounts(rdd, columnIndexes, columnTypes)(ClassTag(rddUnit))
	}

	def getFactorCounts[T](rdd: RDD[T], columnIndexes: JList[JInt], columnTypes: JList[ColumnType])(implicit tag: ClassTag[T]): JMap[JInt, JMap[String, JInt]] = {
		val columnIndexesWithTypes = (columnIndexes zip columnTypes).toList

		tag.runtimeClass match {
			case tp if tp == classOf[TablePartition] ⇒ {
				val mapper = new TablePartitionMapper(columnIndexesWithTypes)
				val rddTP = rdd.asInstanceOf[RDD[TablePartition]]
				rddTP.filter(table ⇒ table.numRows > 0).map(mapper).reduce(new MultiFactorReducer)
			}
			case arrObj if arrObj == classOf[Array[Object]] ⇒ {
				val mapper = new ArraryObjectMultiFactorMapper(columnIndexesWithTypes)
				val rddArrObj = rdd.asInstanceOf[RDD[Array[Object]]]
				rddArrObj.mapPartitions(mapper).reduce(new MultiFactorReducer)
			}
			case _ ⇒ {
				throw new DDFException("Cannot get multi factor for RDD[%s]".format(tag.toString))
			}
		}
	}

	def stringGetter(typ: ColumnType): (Option[Object]) ⇒ Option[String] = typ match {
		case ColumnType.INT ⇒ {
			case Some(ob) ⇒ Option(ob.asInstanceOf[IntWritable].get.toString)
			case None ⇒ None
		}
		case ColumnType.DOUBLE ⇒ {
			case Some(ob) ⇒ Option(ob.asInstanceOf[DoubleWritable].get.toString)
			case None ⇒ None
		}
		case ColumnType.FLOAT ⇒ {
			case Some(ob) ⇒ Option(ob.asInstanceOf[FloatWritable].get.toString)
			case None ⇒ None
		}
		case ColumnType.STRING ⇒ {
			case Some(ob) ⇒ Option(ob.asInstanceOf[Text].toString)
			case None ⇒ None
		}
	}

	class TablePartitionMapper(indexsWithTypes: List[(JInt, ColumnType)])
			extends Function1[TablePartition, JMap[JInt, JMap[String, JInt]]] with Serializable {
		@Override
		def apply(table: TablePartition): JMap[JInt, JMap[String, JInt]] = {
			val aMap: JMap[JInt, JMap[String, JInt]] = new java.util.HashMap[JInt, JMap[String, JInt]]()
			val columnIterators = table.iterator.columnIterators
			val numRows = table.numRows

			//Iterating column base, faster with TablePartition
			for ((idx, typ) ← indexsWithTypes) {
				val newMap = new JHMap[String, JInt]()
				val columnIter = columnIterators(idx)
				var i = 0
				val getter = stringGetter(typ)
				while (i < numRows) {
					columnIter.next()
					val value = getter(Option(columnIter.current))
					value match {
						case Some(string) ⇒ {
							val num = newMap.get(string)
							newMap.put(string, if (num == null) 1 else (num + 1))
						}
						case None ⇒
					}
					i += 1
				}
				aMap.put(idx, newMap)
			}
			aMap
		}
	}

	class ArraryObjectMultiFactorMapper(indexsWithTypes: List[(JInt, ColumnType)])
			extends Function1[Iterator[Array[Object]], Iterator[JMap[JInt, JMap[String, JInt]]]] with Serializable {
		@Override
		def apply(iter: Iterator[Array[Object]]): Iterator[JMap[JInt, JMap[String, JInt]]] = {
			val aMap: JMap[JInt, JMap[String, JInt]] = new java.util.HashMap[JInt, JMap[String, JInt]]()
			while (iter.hasNext) {
				val row = iter.next()
				val typeIter = indexsWithTypes.iterator
				while (typeIter.hasNext) {
					val (idx, typ) = typeIter.next()
					val value: Option[String] = Option(row(idx)) match {
						case Some(x) ⇒ typ match {
							case ColumnType.INT ⇒ Option(x.asInstanceOf[Int].toString)
							case ColumnType.DOUBLE ⇒ Option(x.asInstanceOf[Double].toString)
							case ColumnType.STRING ⇒ Option(x.asInstanceOf[String])
							case ColumnType.FLOAT ⇒ Option(x.asInstanceOf[Float].toString)
							case unknown ⇒ x match {
								case y: java.lang.Integer ⇒ Option(y.toString)
								case y: java.lang.Double ⇒ Option(y.toString)
								case y: java.lang.String ⇒ Option(y)
							}
						}
						case None ⇒ None
					}
					value match {
						case Some(string) ⇒ {
							Option(aMap.get(idx)) match {
								case Some(map) ⇒ {
									val num = map.get(string)
									map.put(string, if (num == null) 1 else num + 1)
								}
								case None ⇒ {
									val newMap = new JHMap[String, JInt]()
									newMap.put(string, 1)
									aMap.put(idx, newMap)
								}
							}
						}
						case None ⇒
					}
				}
			}
			Iterator(aMap)
		}
	}

	class MultiFactorReducer
			extends Function2[JMap[JInt, JMap[String, JInt]], JMap[JInt, JMap[String, JInt]], JMap[JInt, JMap[String, JInt]]]
			with Serializable {
		@Override
		def apply(map1: JMap[JInt, JMap[String, JInt]], map2: JMap[JInt, JMap[String, JInt]]): JMap[JInt, JMap[String, JInt]] = {
			for ((idx, smap1) ← map1) {

				Option(map2.get(idx)) match {
					case Some(smap2) ⇒
						for ((string, num) ← smap1) {
							val aNum = smap2.get(string)
							smap2.put(string, if (aNum == null) num else (aNum + num))
						}
					case None ⇒ map2.put(idx, smap1)
				}
			}
			map2
		}
	}

}
