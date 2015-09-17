package io.ddf.types;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.ddf.exception.DDFException;
import io.ddf.util.Utils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;

public class AggregateTypes {
  public enum AggregateFunction {
    MEAN, AVG, COUNT, SUM, MIN, MAX, MEDIAN, VARIANCE, STDDEV;

    public static AggregateFunction fromString(String s) {
      if (Strings.isNullOrEmpty(s)) return null;

      for (AggregateFunction t : values()) {
        if (t.name().equalsIgnoreCase(s)) return t;
      }

      return null;
    }

    public String toString(String column) {
      switch (this) {
        case MEDIAN:
          return String.format("PERCENTILE_APPROX(%s, 0.5)", column);

        case MEAN:
          return String.format("AVG(%s)", column);

        case AVG:
          return String.format("AVG(%s)", column);

        default:
          return String.format("%s(%s)", this.toString(), column);
      }

    }
  }

  // ////// Inner classes/enums ////////


  public static class AggregationResult extends HashMap<String, double[]> {

    private static final long serialVersionUID = -7809562958792876728L;


    public static AggregationResult newInstance(List<String> sqlResult, int numUnaggregatedFields) {

      AggregationResult result = new AggregationResult();

      for (String res : sqlResult) {

        int pos = StringUtils.ordinalIndexOf(res, "\t", numUnaggregatedFields);
        String groupByColNames = res.substring(0, pos);
        String[] stats = res.substring(pos + 1).split("\t");

        double[] statsDouble = new double[stats.length];

        for (int i = 0; i < stats.length; i++) {
          statsDouble[i] = "null".equalsIgnoreCase(stats[i]) ? Double.NaN : Utils.roundUp(Double.parseDouble(stats[i]));
        }

        result.put(groupByColNames, statsDouble);
      }

      return result;
    }

  }


  /**
   * Represents a field in the aggregation statement "SELECT a, b, SUM(c), MIN(c), MAX(d), COUNT(*) GROUP BY a, b"
   */
  public static class AggregateField {
    public String mColumn;
    public AggregateFunction mAggregateFunction;
    public String mName = "";


    /**
     * An unaggregated column
     *
     * @param column
     */
    public AggregateField(String column) {
      this((AggregateFunction) null, column);
    }

    /**
     * An aggregated column
     *
     * @param column
     * @param aggregationFunction if null, then this is an unaggregated column
     */
    public AggregateField(String aggregateFunction, String column) {
      this(AggregateFunction.fromString(aggregateFunction), column);
    }

    public AggregateField(AggregateFunction aggregateFunction, String column) {
      mColumn = column;
      if (Strings.isNullOrEmpty(mColumn)) mColumn = "*";
      mAggregateFunction = aggregateFunction;
    }

    public boolean isAggregated() {
      return (mAggregateFunction != null);
    }

    public AggregateField setName(String name) {
      mName = name;
      return this;
    }

    public String getColumn() {
      return mColumn;
    }

    public AggregateFunction getAggregateFunction() {
      return mAggregateFunction;
    }

    @Override
    public String toString() {
      // return this.isAggregated() ? this.getAggregateFunction().toString(this.getColumn()) : this.getColumn();
      if (this.isAggregated()) {
        String func = this.getAggregateFunction().toString(this.getColumn());
        return Strings.isNullOrEmpty(mName) ? func : String.format("%s AS %s", func, mName);
      } else return this.getColumn();
    }

    /**
     * Helper method to convert an array of {@link AggregateField}s into a single SELECT statement like
     * "SELECT a, b, SUM(c), MIN(c), MAX(d), COUNT(*) GROUP BY a, b"
     *
     * @param fields
     * @return
     * @throws DDFException
     */
    public static String toSql(List<AggregateField> fields, String tableName) throws DDFException {
      if (fields == null || fields.isEmpty()) {
        throw new DDFException(new UnsupportedOperationException("Field array cannot be null or empty"));
      }

      if (Strings.isNullOrEmpty(tableName)) {
        throw new DDFException("Table name cannot be null or empty");
      }

      return String.format("SELECT %s FROM (%s) tmp GROUP BY %s", toSqlFieldSpecs(fields), tableName,
          toSqlGroupBySpecs(fields));
    }

    /**
     * Converts from a SQL String specs like "a, b, SUM(c), MIN(c)" into an array of SQL {@link AggregateField}s. This
     * is useful for constructing arguments to the {@link aggregate} function.
     *
     * @param sqlFieldSpecs
     * @return null if sqlFieldSpecs is null or empty
     */
    public static List<AggregateField> fromSqlFieldSpecs(String sqlFieldSpecs) {
      if (Strings.isNullOrEmpty(sqlFieldSpecs)) return null;

      String[] specs = sqlFieldSpecs.split(",");
      List<AggregateField> fields = Lists.newArrayList();
      for (String spec : specs) {
        if (Strings.isNullOrEmpty(spec)) continue;
        fields.add(fromFieldSpec(spec));
      }

      return fields;
    }

    public static AggregateField fromFieldSpec(String fieldSpec) {
      if (Strings.isNullOrEmpty(fieldSpec)) return null;

      String spec = fieldSpec.trim();
      String[] parts = spec.split("\\(");
      if (parts.length == 1) {
        return new AggregateField(parts[0]); // just column name
      } else {
        return new AggregateField(parts[0], parts[1].replaceAll("\\)", "")); // function(columnName)
      }
    }

    private static String toSqlFieldSpecs(List<AggregateField> fields) {
      return toSqlSpecs(fields, true);
    }

    private static String toSqlGroupBySpecs(List<AggregateField> fields) {
      return toSqlSpecs(fields, false);
    }

    /**
     * @param fields
     * @param isFieldSpecs If true, include all fields. If false, include only unaggregated fields.
     * @return
     */
    private static String toSqlSpecs(List<AggregateField> fields, boolean isFieldSpecs) {
      List<String> specs = Lists.newArrayList();

      for (AggregateField field : fields) {
        if (isFieldSpecs || !field.isAggregated()) specs.add(field.toString());
      }

      return StringUtils.join(specs.toArray(new String[0]), ',');
    }
  }

}
