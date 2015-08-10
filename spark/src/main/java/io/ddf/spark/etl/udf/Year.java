package io.ddf.spark.etl.udf;


import io.ddf.spark.util.Utils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;

/**
 * A SparkSQL UDF to extract year information from a unixtimestamp
 * or an ISO8601 datetime string.
 * In case of an ISO datetime string with timezone, the output will be a
 * local value at UTC timezone.
 * Created by nhanitvn on 30/07/2015.
 */
public class Year {

  static UDF1 udf = new UDF1<Object, Integer>() {
    @Override public Integer call(Object object) throws Exception {
      DateTime dt = Utils.toDateTimeObject((String) object);

      if (dt != null) {
        return Integer.parseInt(dt.year().getAsString());
      } else {
        return null;
      }
    }
  };

  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("year", udf, DataTypes.IntegerType);
  }
}
