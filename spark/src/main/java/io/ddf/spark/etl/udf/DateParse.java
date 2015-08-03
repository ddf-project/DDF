package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A SparkSQL UDF that convert a datetime string in a specific format into a datetime string
 * of format "yyyy-MM-dd HH:mm:ss". In case of an ISO datetime string with timezone, the output will be a
 * local datetime at UTC timezone.
 * <p>
 * Examples:
 * <p>
 * <ul>
 *  <li>select date_parse(dt, "yyyy-MM-dd'T'HH:mm:ssZ") from table</li>
 *  <li>select date_parse(dt, "iso") from table</li>
 * </ul>
 *
 * Created by nhanitvn on 28/07/2015.
 * @param string the datetime string to convert
 * @param format a string to specify the datetime string's format. A convenient value is "iso" to just specify that
 *               the datetime string is of ISO 8601 format
 * @return a datetime string in new format, null if there is any error.
 */
public class DateParse {

  static UDF2 udf = new UDF2<String, String, String>() {
    @Override public String call(String string, String format) throws Exception {

      try {
        DateTime dt = null;
        if (format.equalsIgnoreCase("iso")) {
          dt = Utils.toDateTimeObject(string);
        } else {
          DateTimeFormatter formatter = DateTimeFormat.forPattern(format).withZoneUTC();
          dt = formatter.parseDateTime(string);
        }
        return dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
      } catch (Exception e) {
        return null;
      }
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("date_parse", udf, DataTypes.StringType);
  }
}
