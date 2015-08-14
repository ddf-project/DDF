package io.ddf.spark.etl

import io.ddf.spark.util.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes

/**
 */
object DateUDF {

  val parseYear: (String => Integer) = (year: String) =>  {
    val datetime = Utils.toDateTimeObject(year)
    if(datetime != null) {
      datetime.getYear
    } else {
      null
    }
  }

  val parseHour: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getHourOfDay
      } else {
        null
      }
    }
  }

  val parseDayOfWeek: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getDayOfWeek
      } else {
        null
      }
    }
  }

  val parseDayOfWeekAsText: (Object, String) => String = {
    (obj: Object, format: String) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        format.toLowerCase() match {
          case "number" => dateTime.getDayOfWeek.toString
          case "text"   => dateTime.dayOfWeek().getAsText
          case "shorttext" => dateTime.dayOfWeek().getAsShortText
        }
      } else {
        null
      }
    }
  }

  val parseMonth: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getMonthOfYear
      } else {
        null
      }
    }
  }

  val parseMonthAsText: (Object, String) => String = {
    (obj: Object, format: String) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        format.toLowerCase match {
          case "number" => dateTime.getMonthOfYear.toString
          case "text" => dateTime.monthOfYear().getAsText()
          case "shorttext" => dateTime.monthOfYear().getAsShortText()
        }
      } else {
        null
      }
    }
  }

  val parseWeekOfYear: (Object) => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime !=null) {
        dateTime.getWeekyear
      } else {
        null
      }
    }
  }

  val parseWeekOfWeekYear: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getWeekOfWeekyear
      } else {
        null
      }
    }
  }

  val parseDay: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime !=null) {
        dateTime.getDayOfMonth
      } else {
        null
      }
    }
  }

  val parseDayOfYear: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getDayOfYear
      } else {
        null
      }
    }
  }

  val parseMinute: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
       dateTime.getMinuteOfHour
      } else {
        null
      }
    }
  }

  val parseSecond: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getSecondOfMinute
      } else {
        null
      }
    }
  }

  val parseMillisecond: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        dateTime.getMillisOfSecond
      } else {
        null
      }
    }
  }

  def registerUDFs(sQLContext: SQLContext) = {
    sQLContext.udf.register("year", parseYear)
    sQLContext.udf.register("month", parseMonth)
    sQLContext.udf.register("month_as_text", parseMonthAsText)
    sQLContext.udf.register("weekyear", parseWeekOfYear)
    sQLContext.udf.register("weekofweekyear", parseWeekOfWeekYear)
    sQLContext.udf.register("day", parseDay)
    sQLContext.udf.register("dayofweek", parseDayOfWeek)
    sQLContext.udf.register("dayofweek_as_text", parseDayOfWeekAsText)
    sQLContext.udf.register("dayofyear", parseDayOfYear)
    sQLContext.udf.register("hour", parseHour)
    sQLContext.udf.register("minute", parseMinute)
    sQLContext.udf.register("second", parseSecond)
    sQLContext.udf.register("millisecond", parseMillisecond)

  }
}
