package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

/**
 * A SparkSQL UDF to extract month information from a unixtimestamp
 * or an ISO8601 datetime string.
 * In case of an ISO datetime string with timezone, the output will be a
 * local value at UTC timezone.
 * @param format: either "number" or "text" or "shorttext" to return a number (1-12)
 *              or a text (January - December)
 *              or a short text (Jan - Dec)
 * Created by nhanitvn on 10/08/2015.
 */
public class Month {
  static UDF2 udf = new UDF2<Object, String, String>() {
    @Override public String call(Object object, String format) throws Exception {

      DateTime dt = Utils.toDateTimeObject(object);

      if (dt != null) {
        if (format.equalsIgnoreCase("number")) {
          return "" + dt.getMonthOfYear();
        } else if (format.equalsIgnoreCase("text")) {
          return dt.monthOfYear().getAsText();
        } else if (format.equalsIgnoreCase("shorttext")) {
          return dt.monthOfYear().getAsShortText();
        }
      }
      return null;
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("month", udf, DataTypes.StringType);
  }
}
