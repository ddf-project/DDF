package io.ddf.spark.etl
import io.ddf.etl.ATimeSeriesHandler;
import io.ddf.etl.IHandleTimeSeries;
import io.ddf.DDF;
import io.ddf.exception.DDFException
import org.apache.spark.sql.expressions.{ WindowSpec, Window }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import io.ddf.spark.util.SparkUtils
import io.ddf.spark.{ SparkDDFManager, SparkDDF }
import java.lang.Math

class TimeSeriesHandler(ddf: DDF) extends ATimeSeriesHandler(ddf) {

  override def addDiffColumn(timestampColumn: String,
    tsIdColumn: String,
    colToGetDiff: String,
    diffColName: String): DDF = {
    val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    var wSpec: WindowSpec = null

    this.setTsIDColumn(tsIdColumn)

    if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
      wSpec = Window.partitionBy(tsIdColumn).orderBy(timestampColumn)
    } else {
      wSpec = Window.orderBy(timestampColumn)
    }

    val prev = lag(colToGetDiff, 1).over(wSpec)

    val newdf = sparkdf.withColumn(diffColName, sparkdf(colToGetDiff) - prev)

    val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
    val res = manager.newDDFFromSparkDataFrame(newdf)
    manager.addDDF(res)
    res

  }

  override def computeMovingAverage(timestampColumn: String,
    tsIdColumn: String,
    colToComputeMovingAverage: String,
    movingAverageColName: String, windowSize: Int): DDF = {

    val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    var wSpec: WindowSpec = null
    val halfWindowSize = Math.floor(windowSize / 2).toInt

    this.setTsIDColumn(tsIdColumn)

    if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
      wSpec = Window.partitionBy(tsIdColumn).orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    } else {
      wSpec = Window.orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    }

    val newdf = sparkdf.withColumn(movingAverageColName, avg(sparkdf(colToComputeMovingAverage)).over(wSpec))

    val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
    val res = manager.newDDFFromSparkDataFrame(newdf)
    manager.addDDF(res)
    res

  }

}