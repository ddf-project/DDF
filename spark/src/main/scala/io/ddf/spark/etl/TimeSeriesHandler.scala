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
import org.apache.spark.rdd.RDD
import io.ddf.spark.content.RepresentationHandler
import io.ddf.spark.content.RDDRow2ArrayDouble
import scala.collection.mutable.ArrayBuffer

class TimeSeriesHandler(ddf: DDF) extends ATimeSeriesHandler(ddf) {

  override def addDiffColumn(timestampColumn: String,
    tsIdColumn: String,
    colToGetDiff: String,
    diffColName: String): DDF = {
    val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]

    this.setTsIDColumn(tsIdColumn)

    val wSpec = if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
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

    val wSpec = if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
      Window.partitionBy(tsIdColumn).orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    } else {
      Window.orderBy(timestampColumn).rowsBetween(halfWindowSize - windowSize + 1, halfWindowSize)
    }

    val newdf = sparkdf.withColumn(movingAverageColName, avg(sparkdf(colToComputeMovingAverage)).over(wSpec))

    val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
    val res = manager.newDDFFromSparkDataFrame(newdf)
    res
  }
  
  override def save_ts(pathToStorage : String) {
    // convert to [tsIdColumn, tuple2(num_feats, num_timesteps), array[double]]
    // tpRddKVPairs
  }
  
  def toRddKVPairs() {
    val keyIdx = ddf.getColumnIndex(this.mTsIDColumn)
    val tsColIdx = ddf.getColumnIndex(this.mTimestampColumn)

    val rdd = ddf.asInstanceOf[SparkDDF].getRDD(classOf[Row])

    val rddKVPairs = rdd.keyBy(row => row.getString(keyIdx))
                       .mapValues( row => {
                         val values = new ArrayBuilder.make[Double]
                         for (i <- 0 to row.length-1) {
                           if (row.isNullAt(i)) return null
                           if (i != tsColIdx && i!= keyIdx) { values += row.getDouble(i)}
                         }
                         (row.getDouble(tsColIdx), values.result)
                       }).groupByKey()  // to get RDD<scala.Tuple2<K,scala.collection.Iterable<V>>>
           
      val rddKV = rddKVPairs.map {}                  
    }
    
    
}