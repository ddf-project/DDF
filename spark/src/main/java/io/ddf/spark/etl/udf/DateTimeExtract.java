package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A SparkSQL UDF that extract date/time field from a unixtimestamp or an ISO8601 datetime string.
 * In case of an ISO datetime string with timezone, the output will be a
 * local datetime at UTC timezone.
 * Support fields: year, month, weekyear, weekofyear, day, dayofweek, dayofyear, hour, minute, second, millisecond
 * Created by nhanitvn on 30/07/2015.
 */
public class DateTimeExtract {

  static UDF2 udf = new UDF2<Object, String, Integer>() {
    @Override public Integer call(Object object, String field) throws Exception {
      DateTime dt = Utils.toDateTimeObject((String) object);
      
      if (dt != null) {
        if (field.equalsIgnoreCase("year")) {
          return new Integer(dt.getYear());
        } else if (field.equalsIgnoreCase("month")) {
          return new Integer(dt.getMonthOfYear());
        } else if (field.equalsIgnoreCase("weekyear")) {
          return new Integer(dt.getWeekyear());
        } else if (field.equalsIgnoreCase("weekofweekyear")) {
          return new Integer(dt.getWeekOfWeekyear());
        } else if (field.equalsIgnoreCase("day")) {
          return new Integer(dt.getDayOfMonth());
        } else if (field.equalsIgnoreCase("dayofweek")) {
          return new Integer(dt.getDayOfWeek());
        } else if (field.equalsIgnoreCase("dayofyear")) {
          return new Integer(dt.getDayOfYear());
        } else if (field.equalsIgnoreCase("hour")) {
          return new Integer(dt.getHourOfDay());
        } else if (field.equalsIgnoreCase("minute")) {
          return new Integer(dt.getMinuteOfHour());
        } else if (field.equalsIgnoreCase("second")) {
          return new Integer(dt.getSecondOfMinute());
        } else if (field.equalsIgnoreCase("millisecond")) {
          return new Integer(dt.getMillisOfSecond());
        }

      }
      return null;
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("extract", udf, DataTypes.IntegerType);
  }
}
