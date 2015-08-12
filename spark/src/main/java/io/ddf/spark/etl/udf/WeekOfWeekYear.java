package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

/**
 * A SparkSQL UDF to extract week of week year information from a unixtimestamp
 * or an ISO8601 datetime string.
 * In case of an ISO datetime string with timezone, the output will be a
 * local value at UTC timezone.
 * This function is normally used together with weekyear UDF
 * Example: weekyear('2015-01-22 20:23 +0000') = 2015
 *          weekofweekyear('2015-01-22 20:23 +0000') = 4
 *
 * Created by nhanitvn on 10/08/2015.
 */
public class WeekOfWeekYear {
  static UDF1 udf = new UDF1<Object, Integer>() {
    @Override public Integer call(Object object) throws Exception {
      DateTime dt = Utils.toDateTimeObject(object);

      if (dt != null) {
        return Integer.parseInt(dt.weekOfWeekyear().getAsString());
      } else {
        return null;
      }
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("weekofweekyear", udf, DataTypes.IntegerType);
  }
}
