package io.ddf.spark.etl
import io.ddf.etl.ATimeSeriesHandler;
import io.ddf.etl.IHandleTimeSeries;
import io.ddf.DDF;
import io.ddf.exception.DDFException
import org.apache.spark.sql.expressions.{WindowSpec, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import io.ddf.spark.util.SparkUtils
import io.ddf.spark.{ SparkDDFManager, SparkDDF }

class TimeSeriesHandler(ddf: DDF) extends ATimeSeriesHandler(ddf) {

  override def addDiffColumn(timestampColumn: String, 
      tsIdColumn: String, 
      columnToGetDiff: String, 
      diffColumn: String) : DDF = {
      val sparkdf = ddf.getRepresentationHandler.get(classOf[DataFrame]).asInstanceOf[DataFrame]
      
      var wSpec: WindowSpec = null
      
      this.setTsIDColumn(tsIdColumn)
      
      if (mTsIDColumn != null && !mTsIDColumn.isEmpty()) {
        wSpec = Window.partitionBy(tsIdColumn).orderBy(timestampColumn)
      } else {
        wSpec = Window.orderBy(timestampColumn)
      }
      
      val prev = lag(columnToGetDiff, 1).over(wSpec)
      
      sparkdf.withColumn(diffColumn, sparkdf(columnToGetDiff) - prev)
      
      val manager = ddf.getManager.asInstanceOf[SparkDDFManager]
      val res = manager.newDDFFromSparkDataFrame(sparkdf)
      manager.addDDF(res)
      res
      
  }
  

}