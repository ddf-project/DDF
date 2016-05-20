package io.ddf.spark.analytics

import java.util

import io.ddf.Factor.FactorBuilder
import io.ddf.content.Schema
import io.ddf.exception.DDFException
import io.ddf.{DDF, Factor}
import io.ddf.content.Schema.{Column, ColumnClass, ColumnType}
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

  /**
    * Transform the DDF, overwriting the old columns
    * @param ddf source DDF
    * @return
    */
  def transform(ddf: DDF): DDF = {
    transform(ddf, null)
  }

  /**
    * Transform the DDF
    * @param ddf source DDF
    * @param newColumnNames a map from old column names to new colum names
    *                       Has to be of equal length with the number of columns with factor maps
    * @return
    */
  def transform(ddf: DDF, newColumnNames: Map[String, String]): DDF = {

    if (categoryMap.isEmpty) {
      return ddf.copy
    }

    val catMapWithColIndex = categoryMap.map {
      case (colName, map) => val colIndex = ddf.getSchemaHandler.getColumnIndex(colName)
        (colIndex, map)
    }

    if (newColumnNames == null) {
      transformOverwrite(ddf, catMapWithColIndex)
    } else {
      transformNewColumns(ddf, catMapWithColIndex, newColumnNames)
    }
  }

  private def transformNewColumns(ddf: DDF, catMapwithColIndex: Map[Int, FactorMap],
                                  newColumnNames: Map[String, String]): DDF = {

    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]

    if (newColumnNames.size != catMapwithColIndex.size) {
      throw new DDFException(s"There are ${newColumnNames.size} new column names, but ${catMapwithColIndex.size} " +
        s"factor columns. Please provide ${catMapwithColIndex.size} new column names")
    }

    val oldColNames = newColumnNames.keys.toSeq

    val duplicatedNewNames = newColumnNames.values.filter(ddf.getColumnNames.contains)
    if (duplicatedNewNames.nonEmpty) {
      throw new DDFException(s"Duplicated new column names: ${duplicatedNewNames.mkString(", ")}")
    }
    val invalidOldColNames = oldColNames.filter(x => !ddf.getColumnNames.contains(x))
    if (invalidOldColNames.nonEmpty) {
      throw new DDFException(s"Invalid old column names: ${invalidOldColNames.mkString(", ")}")
    }

    val numCols = ddf.getNumColumns
    val numNewCols = newColumnNames.size
    val newColNameWithIndex = ddf.getColumnNames.toSeq.zipWithIndex.map {
      case (c, i) => (i, if (oldColNames.contains(c)) oldColNames.indexOf(c) else -1)
    }.toMap

    val transformedRdd = rddRow.mapPartitions {
      iter => iter.map {
        row => {
          val newRow = new Array[Any](numCols + numNewCols)
          for (i <- 0 until numCols) {
            newRow.update(i, row(i))

            catMapwithColIndex.get(i) match {
              case Some(factorMap) =>
                val newIdx = numCols + newColNameWithIndex(i)
                if (row.isNullAt(i)) {
                  newRow.update(newIdx, Double.NaN)
                } else {
                  newRow.update(newIdx, factorMap.value2Index(row(i)))
                }
              case None =>
            }
          }
          Row.fromSeq(newRow)
        }
      }
    }

    val newColumns = newColumnNames.map {
      case (oldName, newName) =>
      categoryMap.get(oldName) match {
        case Some(factorMap) =>
          val newColumn = new Column(newName, factorMap.getTransformedColumnType())
          val factor: Factor[_] = new FactorBuilder().setType(ddf.getColumn(oldName).getType)
            .setLevels(factorMap.values).build()
          newColumn.setAsFactor(factor)
      }
    }
    val newSchema = new Schema(null, ddf.getSchema.getColumns.toBuffer ++ newColumns)
    ddf.getManager.newDDF(transformedRdd, Array(classOf[RDD[_]], classOf[Row]), null, null, newSchema)
  }

  private def transformOverwrite(ddf: DDF, catMapWithColIndex: Map[Int, FactorMap]): DDF = {

    val rddRow = ddf.getRepresentationHandler.get(classOf[RDD[_]], classOf[Row]).asInstanceOf[RDD[Row]]
    val numCols = ddf.getNumColumns

    val transformedRdd = rddRow.mapPartitions {
      iter => {
        iter.map {
          row => {
            val newRow = new Array[Any](numCols)
            var i = 0
            while (i < numCols) {
              catMapWithColIndex.get(i) match {
                case Some(factorMap) =>
                  if (row.isNullAt(i)) {
                    newRow.update(i, Double.NaN)
                  } else {
                    newRow.update(i, factorMap.value2Index(row(i)))
                  }
                case None => newRow.update(i, row(i))
              }
              i += 1
            }
            Row.fromSeq(newRow)
          }
        }
      }
    }

    val newColumns = ddf.getSchema.getColumns.map {
      case column => categoryMap.get(column.getName) match {
        case Some(factorMap) => val newcolumn = new Column(column.getName, factorMap.getTransformedColumnType())
          val levels = factorMap.values
          val factor: Factor[_] = new FactorBuilder().setType(column.getType).setLevels(levels).build()
          newcolumn.setAsFactor(factor)
        case None => column.clone()
      }
    }
    ddf.getManager.newDDF(transformedRdd, Array(classOf[RDD[_]], classOf[Row]), null, null,
      new Schema(null, newColumns))
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
        case Some(factorMap) =>
          val newColumn = new Column(column.getName, factorMap.originalColumnType)
          val levels = factorMap.values
          val factor = new FactorBuilder().setType(newColumn.getType).setLevels(levels).build()
          newColumn.setAsFactor(factor)
        case None => column
      }
    }
    new Schema(newColumns)
  }

  private def inversedTransformRDD(rdd: RDD[Row], categoryMapWithIndex: Map[Int, FactorMap], numCols: Int): RDD[Row] = {
    if (categoryMapWithIndex.isEmpty) {
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
    if (index < values.size) {
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

  def buildModelFromFactorColumns(columns: Array[Column]): FactorIndexerModel = {
    val notFactor = columns.filter(column => column.getColumnClass != ColumnClass.FACTOR).map { col => col.getName }
    if (notFactor.length > 0) {
      throw new DDFException(s"columns ${notFactor.mkString(", ")} are not factor column")
    }
    //    val columnsToBuildModel = columns.filter {
    //      col => (col.getOptionalFactor != null && col.getOptionalFactor.getLevels.isPresent)
    //    }
    val factorMaps = columns.map {
      column =>
        val factor = column.getOptionalFactor
        if (factor.getLevels.isPresent) {
          (column.getName, new FactorMap(factor.getLevels.get(), factor.getType))
        } else {
          throw new DDFException(s"Column ${column.getName} does not contain levels")
        }
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
    if (ddf.getColumn(column).getOptionalFactor == null || !ddf.getColumn(column).getOptionalFactor.getLevels.isPresent) {
      val df = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]
      val columnDDF = df.select(column)
      val counts = columnDDF.map { row => row.get(0) }.countByValue().filter {
        case (value, count) => value != null
      }
      val labels = counts.toSeq.sortBy { case (value, count) => -count }.map(_._1.asInstanceOf[AnyRef]).toList.asJava
      new FactorMap(labels, ddf.getColumn(column).getType)
    } else {
      val labels = ddf.getColumn(column).getOptionalFactor.getLevels.get()
      new FactorMap(labels, ddf.getColumn(column).getType)
    }
  }

  def buildIndexHashMap(values: JList[AnyRef]): java.util.HashMap[Any, Double] = {
    val size = values.size * 1.5
    val map = new util.HashMap[Any, Double](size.toInt)
    var i = 0
    while (i < values.size) {
      map.put(values(i), i)
      i += 1
    }
    map
  }
}

