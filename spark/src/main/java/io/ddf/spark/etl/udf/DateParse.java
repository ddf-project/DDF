package io.ddf.spark.etl.udf;


import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A SparkSQL UDF that convert a datetime string in a specific format into a datetime string
 * of format "yyyy-MM-dd HH:mm:ss" in UTC time zone
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
  static String name = "date_parse";
  static DataType dataTypes = DataTypes.StringType;
  static UDF2 udf = new UDF2<String, String, String>() {
    @Override public String call(String string, String format) throws Exception {

      try {
        DateTime dt = null;
        if (format.equalsIgnoreCase("iso")) {
          dt = new DateTime(string, DateTimeZone.UTC);
        } else {
          DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
          dt = formatter.parseDateTime(string);
        }
        return dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
      } catch (Exception e) {
        return null;
      }
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register(name, udf, dataTypes);
  }
}
