package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A SparkSQL UDF to extract day of week information from a unixtimestamp
 * or an ISO8601 datetime string.
 * In case of an ISO datetime string with timezone, the output will be a
 * local value at UTC timezone.
 * @param format: either "number" or "text" or "shorttext" to return a number (1-7)
 *              or a text (Monday - Sunday)
 *              or a short text (Mon - Sun)
 * Created by nhanitvn on 08/08/2015.
 */
public class DayOfWeek {
  static UDF2 udf = new UDF2<Object, String, String>() {
    @Override public String call(Object object, String format) throws Exception {

      DateTime dt = Utils.toDateTimeObject((String) object);

      if (dt != null) {
        if (format.equalsIgnoreCase("number")) {
          return "" + dt.getDayOfWeek();
        } else if (format.equalsIgnoreCase("text")) {
          return dt.dayOfWeek().getAsText();
        } else if (format.equalsIgnoreCase("shorttext")) {
          return dt.dayOfWeek().getAsShortText();
        }
      }
      return null;
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("dayofweek", udf, DataTypes.StringType);
  }
}
