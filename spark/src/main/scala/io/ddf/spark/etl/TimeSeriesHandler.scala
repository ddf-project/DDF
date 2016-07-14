package io.ddf.spark.etl
import io.ddf.etl.ATimeSeriesHandler
import io.ddf.DDF
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row}
import io.ddf.spark.{SparkDDF, SparkDDFManager}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types
import org.apache.spark.mllib.linalg.VectorUDT

class TimeSeriesHandler(ddf: DDF) extends ATimeSeriesHandler(ddf) {

  override def addDiffColumn(timestampColumn: String,
    tsIdColumn: String,
    colToGetDiff: String,
    diffColName: String): DDF = {
    val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    this.setTsIDColumn(tsIdColumn)

    val wSpec = if (mTsIDColumn != null && !mTsIDColumn.isEmpty) {
      Window.partitionBy(tsIdColumn).orderBy(timestampColumn)
    } else {
      Window.orderBy(timestampColumn)
    }

    val prev = lag(colToGetDiff, 1).over(wSpec)

    val newdf = sparkdf.withColumn(diffColName, sparkdf(colToGetDiff) - prev)

    val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
    val res = manager.newDDFFromSparkDataFrame(newdf)
    res
  }

  override def computeMovingAverage(timestampColumn: String,
    tsIdColumn: String,
    colToComputeMovingAverage: String,
    movingAverageColName: String, windowSize: Int): DDF = {

    val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    val halfWindowSize = Math.floor(windowSize / 2).toInt

    this.setTsIDColumn(tsIdColumn)

    val wSpec = if (mTsIDColumn != null && !mTsIDColumn.isEmpty) {
      Window.partitionBy(tsIdColumn).orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    } else {
      Window.orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    }

    val newdf = sparkdf.withColumn(movingAverageColName, avg(sparkdf(colToComputeMovingAverage)).over(wSpec))

    val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
    val res = manager.newDDFFromSparkDataFrame(newdf)
    res
  }

  /**
    *
    * @param groupByColumns: columns to group by the data
    * @param sortByColumn: column to sort the time series data by
    * @param featureColumns: feature columns of the time series data
    * @example
    *          stockSymbol | timestamp | feature_1 | feature_2 | feature_3
    *          A           | 1         | 100       | 100.1     | 10
    *          A           | 2         | 99        | 95        | 10.5
    *          B           | 1         | 60        | 61        | 100
    *          B           | 2         | 63        | 64        | 105
    *    flatten(Array(stockSymbol), timestamp, Array(feature_1, feature_2, feature_3)) will return an RDD with the
    *    following structure
    *          stockSymbol |  feature_size  |  features
    *          A           |  3             | Array(100, 100.1, 10, 99, 95, 10.5)
    *          B           |  3             | Array(60, 61, 100, 63, 64, 105)
    * @return
    */
  def flatten(groupByColumns: Array[String], sortByColumn: String, featureColumns: Array[String]): DDF = {
    val dataFrame = ddf.asInstanceOf[SparkDDF].getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    val columns: Array[Column] = ((groupByColumns :+ sortByColumn) ++ featureColumns).map{
      col => dataFrame.col(col)
    }

    val projectedDF = dataFrame.select(columns: _*)

    val sortByColumnIndex = projectedDF.schema.fieldIndex(sortByColumn)

    val groupByColumnIndexes = groupByColumns.map {
      col => projectedDF.schema.fieldIndex(col)
    }
    val featureColumnIndexes = featureColumns.map{
      col => projectedDF.schema.fieldIndex(col)
    }

    val featureSize = featureColumns.size
    val rddRow = projectedDF.rdd

    val groupedRDD: RDD[(String, Iterable[Row])] = rddRow.groupBy(
      row =>
        groupByColumnIndexes.map(
          col => row.getString(col)).mkString(", ")
    )

    val resultRDD = groupedRDD.map {
      case (key, values) =>
        val sortedValues = values.toArray.sortBy(row => row.getLong(sortByColumnIndex))
        val bigArray = mutable.ArrayBuilder.make[Double]
        var i = 0
        while(i < sortedValues.size) {
          val row = sortedValues(i)
          var j = 0
          while(j < featureSize) {
            val d = row.getDouble(featureColumnIndexes(j))
            bigArray += d
            j += 1
          }
          i += 1
        }
        Row(key, featureSize, Vectors.dense(bigArray.result()))
    }
    val keyColumn = new StructField("key", types.StringType)
    val featureSizeColumn = new StructField("featureSize", types.IntegerType)
    val featuresColumn = new StructField("features", new VectorUDT)
    val schema = StructType(Array(keyColumn, featureSizeColumn, featuresColumn))
    val sparkDDFManager = this.getManager.asInstanceOf[SparkDDFManager]
    val df = sparkDDFManager.getHiveContext.createDataFrame(resultRDD, schema)
    sparkDDFManager.newDDFFromSparkDataFrame(df)
  }
}
