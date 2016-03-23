package io.ddf.spark.analytics

import io.ddf.DDF
import io.ddf.analytics.{AggregationHandler=>CoreAggregationHandler}
import io.ddf.exception.DDFException
import java.util.{List=>JList}

import io.ddf.spark.util.SparkUtils
import io.ddf.types.AggregateTypes
import io.ddf.types.AggregateTypes.{AggregateField, AggregationResult}
import org.apache.spark.sql.AnalysisException

import scala.collection.JavaConverters._

/**
 * Created by nhanitvn on 3/21/16.
 */
class AggregationHandler(theDDF: DDF) extends CoreAggregationHandler(theDDF) {
  override def groupBy(groupedColumns: JList[String], aggregateFunctions: JList[String]): DDF = {
    // Catch SparkSQL/JSQLParser error message, parse and produce appropriate error message
    try {
      super.groupBy(groupedColumns, aggregateFunctions)
    } catch {
      case ddfException: DDFException => throw new DDFException(SparkUtils.sqlErrorToDDFError(ddfException.getMessage, buildGroupBySQL(aggregateFunctions), aggregateFunctions.asScala.toList:::groupedColumns.asScala.toList))
    }
  }

  override def aggregate (fields: JList[AggregateTypes.AggregateField]): AggregateTypes.AggregationResult = {
    // Catch SparkSQL/JSQLParser error message, parse and produce appropriate error message
    try {
      super.aggregate(fields)
    } catch {
      case ddfException: DDFException =>
        val tableName: String = this.getDDF.getTableName
        throw new DDFException(SparkUtils.sqlErrorToDDFError(ddfException.getMessage, AggregateField.toSql(fields, tableName), fields.asScala.toList.map(_.toString)))
    }
  }
}
