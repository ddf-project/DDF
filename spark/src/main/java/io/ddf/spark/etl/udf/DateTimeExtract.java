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
 * localized value wrt the corresponding timezone.
 * Support fields: year, month, day, dayofweek, dayofyear, hour, minute, second, millisecond
 * Created by nhanitvn on 30/07/2015.
 */
public class DateTimeExtract {

  static UDF2 udf = new UDF2<Object, String, Integer>() {
    @Override public Integer call(Object object, String field) throws Exception {
      DateTime dt = null;
      if (object instanceof Integer) {
        // Unix timestamp
        dt = new DateTime((Integer) object * 1000L).withZone(DateTimeZone.UTC);
      } else {
        dt = Utils.toDateTimeObject((String) object);
      }

      Integer value = null;
      if (dt != null) {
        if (field.equalsIgnoreCase("year")) {
          value = new Integer(dt.getYear());
        } else if (field.equalsIgnoreCase("month")) {
          value = new Integer(dt.getMonthOfYear());
        } else if (field.equalsIgnoreCase("day")) {
          value = new Integer(dt.getDayOfMonth());
        } else if (field.equalsIgnoreCase("dayofweek")) {
          value = new Integer(dt.getDayOfWeek());
        } else if (field.equalsIgnoreCase("dayofyear")) {
          value = new Integer(dt.getDayOfYear());
        } else if (field.equalsIgnoreCase("hour")) {
          value = new Integer(dt.getHourOfDay());
        } else if (field.equalsIgnoreCase("minute")) {
          value = new Integer(dt.getMinuteOfHour());
        } else if (field.equalsIgnoreCase("second")) {
          value = new Integer(dt.getSecondOfMinute());
        } else if (field.equalsIgnoreCase("millisecond")) {
          value = new Integer(dt.getMillisOfSecond());
        }

      }
      return value;
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("extract", udf, DataTypes.IntegerType);
  }
}
