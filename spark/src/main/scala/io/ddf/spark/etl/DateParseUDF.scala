package io.ddf.spark.etl

import io.ddf.spark.util.Utils
import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormat
;
/**
 */
object DateParseUDF {
  val parseDate: (String, String) => String = {
    (string: String, format: String) => {
      try {
        val dateTime = if (format.equalsIgnoreCase("iso")) {
          Utils.toDateTimeObject(string)
        } else {
          val formatter = DateTimeFormat.forPattern(format)
          formatter.parseDateTime(string)
        }
        dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      } catch {
        case e: Throwable => null
      }
    }
  }

  def register(sQLContext: SQLContext) = {
    sQLContext.udf.register("date_parse", parseDate)
  }
}
