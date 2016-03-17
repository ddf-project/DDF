package io.ddf.spark.analytics

import java.util

import io.ddf.content.Schema
import io.ddf.exception.DDFException
import io.ddf.{Factor, DDF}
import io.ddf.content.Schema.{ColumnClass, ColumnType, Column}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}

class FactorIndexerModel(categoryMap: Map[String, FactorMap]) {

  // for MLPredictMethod generic invoking
  def predict(ddf: DDF): DDF = {
    this.transform(ddf)
  }

  def getColumnNames(): Array[String] = categoryMap.keys.toArray

  def getFactorMapforColumn(column: String): Option[FactorMap] = {
    categoryMap.get(column)
  }

  def transform(ddf: DDF): DDF = {

    val catMapwithColIndex = categoryMap.map {
      case (colName, map) => val colIndex = ddf.getSchemaHandler.getColumnIndex(colName)
        (colIndex, map)
    }
    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val transformedRDD = this.transformRDD(rddRow, catMapwithColIndex, ddf.getNumColumns)
    val schema = transformSchema(ddf.getSchema)
    ddf.getManager.newDDF(transformedRDD, Array(classOf[RDD[_]], classOf[Row]), null, null, schema)
  }

  private def transformSchema(schema: Schema): Schema = {
    val newColumns = schema.getColumns.map {
      case column => categoryMap.get(column.getName) match {
        case Some(factorMap) => val newcolumn = new Column(column.getName, factorMap.getTransformedColumnType())
          newcolumn.setAsFactor(column.getOptionalFactor)
        case None => column.clone()
      }
    }
    new Schema(newColumns)
  }

  private def transformRDD(input: RDD[Row], categoryMapWithIndex: Map[Int, FactorMap], numCols: Int): RDD[Row] = {
    if(categoryMapWithIndex.size == 0) {
      input
    } else {
      input.mapPartitions {
        iter => {
          val arrBuffer = new ArrayBuffer[Row]()
          while (iter.hasNext) {
            val row = iter.next
            val newRow = new Array[Any](numCols)
            var i = 0
            while (i < numCols) {
              categoryMapWithIndex.get(i) match {
                case Some(factorMap) => {
                  if (row.isNullAt(i)) {
                    newRow.update(i, Double.NaN)
                  } else {
                    newRow.update(i, factorMap.value2Index(row(i)))
                  }
                }
                case None => newRow.update(i, row(i))
              }
              i += 1
            }
            arrBuffer += Row.fromSeq(newRow)
          }
          arrBuffer.toIterator
        }
      }
    }
  }

  def inversedTransform(ddf: DDF): DDF = {
    val catMapwithColIndex = categoryMap.map {
      case (colName, map) => val colIndex = ddf.getSchemaHandler.getColumnIndex(colName)
        (colIndex, map)
    }
    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val transformedRDD = this.inversedTransformRDD(rddRow, catMapwithColIndex, ddf.getNumColumns)

    val schema = inversedTransformSchema(ddf.getSchema)
    ddf.getManager.newDDF(transformedRDD, Array(classOf[RDD[_]], classOf[Row]), null, null, schema)
  }

  private def inversedTransformSchema(schema: Schema): Schema = {
    val newColumns = schema.getColumns.map {
      case column => categoryMap.get(column.getName) match {
        case Some(factorMap) => new Column(column.getName, factorMap.originalColumnType)
        case None => column
      }
    }
    new Schema(newColumns)
  }

  private def inversedTransformRDD(rdd: RDD[Row], categoryMapWithIndex: Map[Int, FactorMap], numCols: Int): RDD[Row] = {
    if (categoryMapWithIndex.size == 0) {
      rdd
    } else {
      rdd.mapPartitions {
        iter => {
          val arrBuffer = new ArrayBuffer[Row]()
          while (iter.hasNext) {
            val row = iter.next
            val newRow = new Array[Any](numCols)
            var i = 0
            while (i < numCols) {
              categoryMapWithIndex.get(i) match {
                case Some(factorMap) => {
                  if (row.isNullAt(i)) {
                    newRow.update(i, Double.NaN)
                  } else {
                    newRow.update(i, factorMap.index2Value(row.getDouble(i)))
                  }
                }
                case None => newRow.update(i, row(i))
              }
              i += 1
            }
            arrBuffer += Row.fromSeq(newRow)
          }
          arrBuffer.toIterator
        }
      }
    }
  }
}

class FactorMap(val values: JList[AnyRef], val originalColumnType: ColumnType)
  extends Serializable {

  val value2Index: java.util.HashMap[Any, Double] = FactorIndexer.buildIndexHashMap(values)

  def value2Index(value: Any): Double = {
    value2Index.get(value)
  }

  def index2Value(index: Double): Any = {
    if(index < values.size)  {
      values(index.toInt)
    } else {
      null
    }
  }

  def getTransformedColumnType(): ColumnType = {
    ColumnType.DOUBLE
  }
}

object FactorIndexerModel {
  def apply(columns: Array[Column]): FactorIndexerModel = {
    val notFactor = columns.filter(column => column.getColumnClass != ColumnClass.FACTOR).map{col => col.getName}
    if(notFactor.size > 0) {
      throw new DDFException(s"columns ${notFactor.mkString(", ")} are not factor column")
    }

    val factorMaps = columns.map{
      column =>
        val factor = column.getOptionalFactor
        (column.getName, new FactorMap(factor.getLevels, factor.getType))
    }.toMap
    new FactorIndexerModel(factorMaps)
  }
}

object FactorIndexer {

  def fit(ddf: DDF, columns: Array[String]): FactorIndexerModel = {
    val genericFactorMaps = columns.map {
      column => (column, getFactorMapForColumn(ddf, column))
    }.toMap
    new FactorIndexerModel(genericFactorMaps)
  }

  def getFactorMapForColumn(ddf: DDF, column: String): FactorMap = {
    val df = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]
    val columnDDF = df.select(column)
    val counts = columnDDF.map{row => row.get(0)}.countByValue()
    val labels = counts.toSeq.sortBy{case (value, count) => count}.map(_._1.asInstanceOf[AnyRef]).toList.asJava
    new FactorMap(labels, ddf.getColumn(column).getType)
  }

  def buildIndexHashMap(values: JList[AnyRef]): java.util.HashMap[Any, Double] = {
    val size = values.size * 1.5
    val map = new util.HashMap[Any, Double](size.toInt)
    var i = 0
    while(i < values.size) {
      map.put(values(i), i)
      i += 1
    }
    map
  }
}

