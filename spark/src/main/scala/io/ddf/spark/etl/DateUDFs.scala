package io.ddf.spark.etl

import io.ddf.spark.util.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes

/**
 */
object DateUDF {

  val parseYear: (Object => Integer) =  {
    (obj: Object) => {
      val datetime = Utils.toDateTimeObject(obj)
      if(datetime != null) {
        datetime.getYear
      } else {
        null
      }
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

  val parseQuarter: Object => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime != null) {
        val month = dateTime.getMonthOfYear
        if (month >= 1 && month <= 3) {
          1
        } else if (month >= 4 && month <= 6) {
          2
        } else if (month >= 7 && month <= 9) {
          3
        } else if (month >= 10 && month <= 12) {
          4
        } else {
          null
        }

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

  val parseWeekYear: (Object) => Integer = {
    (obj: Object) => {
      val dateTime = Utils.toDateTimeObject(obj)
      if(dateTime !=null) {
        dateTime.getWeekyear
      } else {
        null
      }
    }
  }

  val parseWeekOfYear: Object => Integer = {
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
    sQLContext.udf.register("quarter", parseQuarter)
    sQLContext.udf.register("month", parseMonth)
    sQLContext.udf.register("month_as_text", parseMonthAsText)
    sQLContext.udf.register("weekyear", parseWeekYear)
    sQLContext.udf.register("weekofyear", parseWeekOfYear)
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
