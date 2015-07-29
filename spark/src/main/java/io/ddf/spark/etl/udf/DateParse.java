package io.ddf.spark.etl.udf;


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
 * local datetime string in the corresponding timezone.
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
          Pattern pattern = Pattern.compile(
              "^(?<year>-?(?:[1-9][0-9]*)?[0-9]{4})(-(?<month>1[0-2]|0[1-9])(-"
                  + "(?<day>3[01]|0[1-9]|[12][0-9])((?<sep>[T\\s])(?<hour>2[0-3]|[01][0-9])"
                  + "(?<minute>:[0-5][0-9])((?<second>:[0-5][0-9])?(?<ms>\\.[0-9]+)?)?"
                  + "(?<timezone>Z|\\s?[+-](?:2[0-3]|[01][0-9])(:?[0-5][0-9])?)?)?)?)?$"
          );
          Matcher matcher= pattern.matcher(string);
          if (matcher.matches()) {
            String year = matcher.group("year");
            String month = matcher.group("month");
            String day = matcher.group("day");
            String sep = matcher.group("sep");
            String hour = matcher.group("hour");
            String minute = matcher.group("minute");
            String second = matcher.group("second");
            String ms = matcher.group("ms");
            String timezone = matcher.group("timezone");

            StringBuilder sb = new StringBuilder();
            sb.append(year);
            sb.append(month!=null?("-"+month):"");
            sb.append(day!=null?("-"+day):"");
            sb.append(sep!=null?sep:"");
            sb.append(hour!=null?hour:"");
            sb.append(minute!=null?(minute):"");
            sb.append(second!=null?(second):"");
            sb.append(ms!=null?(ms):"");
            sb.append(timezone!=null?timezone:"");

            StringBuilder sb2 = new StringBuilder();
            sb2.append("yyyy");
            sb2.append(month!=null?("-"+"MM"):"");
            sb2.append(day!=null?("-"+"dd"):"");
            sb2.append(sep!=null?sep.equalsIgnoreCase("T")?"'T'":sep:"");
            sb2.append(hour!=null?"HH":"");
            sb2.append(minute!=null?(":" + "mm"):"");
            sb2.append(second!=null?(":" + "ss"):"");
            sb2.append(ms!=null?("."+"SSS"):"");
            sb2.append(timezone!=null?timezone.startsWith(" ")?" Z":"Z":"");

            DateTimeFormatter formatter = DateTimeFormat.forPattern( sb2.toString());
            dt = formatter.parseDateTime(sb.toString());

          } else {
            return null;
          }

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
