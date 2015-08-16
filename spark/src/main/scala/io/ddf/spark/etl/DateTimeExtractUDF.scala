package io.ddf.spark.etl

import io.ddf.spark.util.Utils
import org.apache.spark.sql.SQLContext

/**
 */
object DateTimeExtractUDF {
  val extractDateTime: (Object, String) => Integer =  {
    (obj: Object, field: String) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        val intValue = field.toLowerCase match {
          case "year" => dateTime.getYear
          case "month" => dateTime.getMonthOfYear
          case "weekyear" => dateTime.getWeekyear
          case "weekofweekyear" => dateTime.getWeekOfWeekyear
          case "day" => dateTime.getDayOfMonth
          case "dayofweek" => dateTime.getDayOfWeek
          case "dayofyear" => dateTime.getDayOfYear
          case "hour" => dateTime.getHourOfDay
          case "minute" => dateTime.getMinuteOfHour
          case "second" => dateTime.getSecondOfMinute
          case "millisecond" => dateTime.getMillisOfSecond
        }
        new Integer(intValue)
      } else {
        null
      }
    }
  }

  def register(sQLContext: SQLContext) = {
    sQLContext.udf.register("extract", extractDateTime)
  }
}
