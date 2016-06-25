package io.ddf.spark.analytics;


import io.ddf.DDF;
import io.ddf.analytics.*;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnType;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.spark.SparkDDFManager;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compute the basic statistics for each column in a RDD-based DDF
 */
public class BasicStatisticsComputer extends AStatisticsSupporter {

  public BasicStatisticsComputer(DDF theDDF) {
    super(theDDF);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Summary[] getSummaryImpl() throws DDFException {

    DataFrame df = (DataFrame) this.getDDF().getRepresentationHandler().get(DataFrame.class);
    DataFrame summaryDF = df.describe();
    Row[] rows = summaryDF.collect();
    Summary[] summaries = new Summary[df.columns().length];
    List<Column> columns = this.getDDF().getSchema().getColumns();

    int colIndex = 0;
    int i = 1;
    for(Column column: columns) {
      if(column.isNumeric()) {

        long count = 0L;
        if(rows[0].isNullAt(i)) {
          count = 0L;
        } else {
          count = Long.valueOf(rows[0].getString(i));
        }

        Double mean = Double.NaN;
        if(!rows[1].isNullAt(i)) {
          mean = Double.valueOf(rows[1].getString(i));
        }

        Double sqdeviation = Double.NaN;
        if(!rows[2].isNullAt(i)) {
          Double stddev = Double.valueOf(rows[2].getString(i));
          sqdeviation = Math.pow(stddev, 2)*(count - 1);
        }

        Double min = Double.NaN;
        if(!rows[3].isNullAt(i)) {
          min = Double.valueOf(rows[3].getString(i));
        }

        Double max = Double.NaN;
        if(!rows[4].isNullAt(i)) {
          max = Double.valueOf(rows[4].getString(i));
        }

        String sqlCmd = String.format("select count(*) from %s where `%s` is null",
            this.getDDF().getTableName(), column.getName());
        long naCount = df.sqlContext().sql(sqlCmd).collect()[0].getLong(0);
        Summary summary = new Summary(count, mean, sqdeviation, naCount, min, max);
        summaries[colIndex] = summary;
        i += 1;
      } else {
        // summary = null if not numeric
        summaries[colIndex] = null;
      }
      colIndex += 1;
    }
    return summaries;
  }

  @Override
  public SimpleSummary[] getSimpleSummaryImpl() throws DDFException {
    List<Column> categoricalColumns = this.getCategoricalColumns();

    List<SimpleSummary> simpleSummaries = new ArrayList<>();
    HiveContext sqlContext = ((SparkDDFManager) this.getDDF().getManager()).getHiveContext();
    for(Column column: categoricalColumns) {
      String sqlCmd = String.format("select distinct(%s) from %s where %s is not null", column.getName(), this.getDDF().getTableName(), column.getName());
      DataFrame sqlresult = sqlContext.sql(sqlCmd);
      Row[] rows = sqlresult.collect();
      List<String> values = new ArrayList<>();
      for(Row row: rows) {
        values.add(row.apply(0).toString());
      }

      CategoricalSimpleSummary summary = new CategoricalSimpleSummary();
      summary.setValues(values);
      summary.setColumnName(column.getName());
      simpleSummaries.add(summary);
    }

    List<Column> numericColumns = this.getNumericColumns();
    List<String> sqlCommand = new ArrayList<>();
    System.out.println(">>> numbericColumn.size = " + numericColumns.size());
    for(Column column: numericColumns) {
      sqlCommand.add(String.format("min(%s), max(%s)", column.getName(), column.getName()));
    }
    String sql = StringUtils.join(sqlCommand, ", ");
    sql = String.format("select %s from %s", sql, this.getDDF().getTableName());
    DataFrame sqlResult = sqlContext.sql(sql);
    Row[] rows = sqlResult.collect();
    Row result = rows[0];
    int i = 0;
    for(Column column: numericColumns) {
      NumericSimpleSummary summary = new NumericSimpleSummary();
      summary.setColumnName(column.getName());
      if(result.isNullAt(i)) {
        summary.setMin(Double.NaN);
      } else {
        summary.setMin(Double.parseDouble(result.get(i).toString()));
      }

      if(result.isNullAt(i + 1)) {
        summary.setMax(Double.NaN);
      } else {
        summary.setMax(Double.parseDouble(result.get(i + 1).toString()));
      }
      simpleSummaries.add(summary);
      i += 2;
    }
    return simpleSummaries.toArray(new SimpleSummary[simpleSummaries.size()]);
  }

  private List<Column> getCategoricalColumns() {
    List<Column> columns = new ArrayList<Column>();
    for(Column column: this.getDDF().getSchemaHandler().getColumns()) {
      if(column.getColumnClass() == Schema.ColumnClass.FACTOR) {
        columns.add(column);
      }
    }
    return columns;
  }

  private List<Column> getNumericColumns() {
    List<Schema.ColumnType> numerics = Arrays.asList(ColumnType.BIGINT, ColumnType.DOUBLE, ColumnType.INT, ColumnType.FLOAT);
    List<Column> columns = new ArrayList<Column>();
    for(Column column: this.getDDF().getSchemaHandler().getColumns()) {
      if(numerics.contains(column.getType())) {
        columns.add(column);
      }
    }
    return columns;
  }

  /**
   * Mapper function to accumulate summary data from each row
   */
  @SuppressWarnings("serial")
  public static class GetSummaryMapper implements Function<Object[], Summary[]> {
    private final Logger mLog = LoggerFactory.getLogger(this.getClass());

    @Override
    public Summary[] call(Object[] p) {
      int dim = p.length;
      if (p != null && dim > 0) {
        Summary[] result = new Summary[dim];
        for (int i = 0; i < dim; i++) {
          Summary s = new Summary();
          if(p[i] == null) {
            result[i] = new Summary();
            result[i].setNACount(1);
          } else if (p[i] instanceof Double) {
            Double a = (Double) p[i];
            result[i] = s.merge(a);
          } else if (p[i] instanceof Integer) {
            Double a = Double.parseDouble(p[i].toString());
            result[i] = s.merge(a);
          } else if (p[i] instanceof Long) {
            Double a = Double.parseDouble(p[i].toString());
            result[i] = s.merge(a);
          } else if (p[i] != null) {
            String str = p[i].toString();
            if (str.trim().equalsIgnoreCase("NA")) {
              result[i] = new Summary();
              result[i].setNACount(1);
            } else {
              if (NumberUtils.isNumber(str)) {
                double number = Double.parseDouble(str);
                s.merge(number);
              } else {
                result[i] = null;
              }
            }
          }
        }
        return result;
      } else {
        mLog.error("malformed line input");
        return null;
      }
    }
  }


  @SuppressWarnings("serial")
  public static class GetSummaryReducer implements Function2<Summary[], Summary[], Summary[]> {
    @Override
    public Summary[] call(Summary[] a, Summary[] b) {
      int dim = a.length;
      Summary[] result = new Summary[dim];

      for (int i = 0; i < dim; i++) {
        // normal cases
        if (a[i] != null && b[i] != null) {
          if (!a[i].isNA() && !b[i].isNA()) {
            result[i] = a[i].merge(b[i]);
          } else if (!a[i].isNA()) {
            result[i] = a[i];
            result[i].addToNACount(b[i].NACount());
          } else if (!b[i].isNA()) {
            result[i] = b[i];
            result[i].addToNACount(a[i].NACount());
          }
          // both are NAs
          else {
            result[i] = new Summary();
            result[i].setNACount(a[i].NACount() + b[i].NACount());
          }
        } else {
          if (a[i] != null) {
            result[i] = new Summary();
            result[i] = a[i];
            result[i].addToNACount(1);
          } else if (b[i] != null) {
            result[i] = new Summary();
            result[i] = b[i];
            result[i].addToNACount(1);
          }
        }
      }
      return result;
    }
  }
}
