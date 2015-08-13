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

  val parseDayOfWeek: (Object, String) => String = {
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

  val parseMonth: (Object, String) => String = {
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

  def registerYearUDF(sQLContext: SQLContext) = {
    sQLContext.udf.register("year", parseYear)
  }

  def registerHourUDF(sqlContext: SQLContext) = {
    sqlContext.udf.register("hour", parseHour)
  }

  def registerDayOfWeekUDf(sqlContext: SQLContext) = {
    sqlContext.udf.register("dayofweek", parseDayOfWeek)
  }

  def registerMonthUDF(sqlContext: SQLContext) = {
    sqlContext.udf.register("month", parseMonth)
  }

  def registerWeekOfYearUDF(sQLContext: SQLContext) = {
    sQLContext.udf.register("weekyear", parseWeekOfYear)
  }

  def registerWeekOfWeekYear(sQLContext: SQLContext) = {
    sQLContext.udf.register("weekofweekyear", parseWeekOfWeekYear)
  }

  def registerDay(sQLContext: SQLContext) = {
    sQLContext.udf.register("day", parseDay)
  }

  def registerDayOfYear(sQLContext: SQLContext) = {
    sQLContext.udf.register("dayofyear", parseDayOfYear)
  }

  def registerMinute(sQLContext: SQLContext) = {
    sQLContext.udf.register("minute", parseMinute)
  }

  def registerSecond(sQLContext: SQLContext) = {
    sQLContext.udf.register("second", parseSecond)
  }

  def registerMillisecond(sQLContext: SQLContext) = {
    sQLContext.udf.register("millisecond", parseMillisecond)
  }
}
