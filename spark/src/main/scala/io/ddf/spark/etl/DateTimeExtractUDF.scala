package io.ddf.spark.etl

import io.ddf.spark.util.Utils
import org.apache.spark.sql.SQLContext

/**
 */
object DateTimeExtractUDF {
  val extractDateTime: (Object, String) => Integer =  {
    (obj: Object, field: String) => {
      field.toLowerCase match {
          case "year" => DateUDF.parseYear(obj)
          case "month" => DateUDF.parseMonth(obj)
          case "quarter" => DateUDF.parseQuarter(obj)
          case "weekyear" => DateUDF.parseWeekYear(obj)
          case "weekofyear" => DateUDF.parseWeekOfYear(obj)
          case "day" => DateUDF.parseDay(obj)
          case "dayofmonth" => DateUDF.parseDay(obj)
          case "dayofweek" => DateUDF.parseDayOfWeek(obj)
          case "dayofyear" => DateUDF.parseDayOfYear(obj)
          case "hour" => DateUDF.parseHour(obj)
          case "minute" => DateUDF.parseMinute(obj)
          case "second" => DateUDF.parseSecond(obj)
          case "millisecond" => DateUDF.parseMillisecond(obj)
          case _ => null
      }
    }
  }

  def register(sQLContext: SQLContext) = {
    sQLContext.udf.register("extract", extractDateTime)
  }
}
