package io.ddf.analytics;


import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.types.AggregateTypes.AggregateField;
import io.ddf.types.AggregateTypes.AggregateFunction;
import io.ddf.types.AggregateTypes.AggregationResult;
import io.ddf.util.Utils;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class AggregationHandler extends ADDFFunctionalGroupHandler implements IHandleAggregation {

  private List<String> mGroupedColumns;


  public AggregationHandler(DDF theDDF) {
    super(theDDF);
  }


  public static class FiveNumSumary implements Serializable {

    private static final long serialVersionUID = -2810459228746952242L;

    // private double mMin = Double.MAX_VALUE;
    // private double mMax = Double.MIN_VALUE;
    // private double first_quantile;
    // private double median;
    // private double third_quantile;

  }


  public FiveNumSumary getFiveNumSumary() {
    // String cmd;
    return null;
  }

  @Override
  public double computeCorrelation(String columnA, String columnB) throws DDFException {
    if (!(this.getDDF().getColumn(columnA).isNumeric() || this.getDDF().getColumn(columnB).isNumeric())) {
      throw new DDFException("Only numeric fields are accepted!");
    }

    String sqlCmd = String.format("SELECT CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF().getTableName());
    try {
      List<String> rs = this.getManager().sql(sqlCmd, this.getEngine()).getRows();
      return Utils.roundUp(Double.parseDouble(rs.get(0)));

    } catch (Exception e) {
      throw new DDFException(String.format("Unable to get CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF()
          .getTableName()), e);
    }
  }

  /**
   * Performs the equivalent of a SQL aggregation statement like "SELECT year, month, AVG(depdelay), MIN(arrdelay) FROM
   * airline GROUP BY year, month"
   *
   * @param fields {@link AggregateField}s representing a list of column specs, some of which may be aggregated, while other
   *               non-aggregated fields are the GROUP BY keys
   * @return
   * @throws DDFException
   */
  @Override
  public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {

    String tableName = this.getDDF().getTableName();

    String sqlCmd = AggregateField.toSql(fields, tableName);
    mLog.info("SQL Command: " + sqlCmd);
    int numUnaggregatedFields = 0;

    for (AggregateField field : fields) {
      if (!field.isAggregated()) numUnaggregatedFields++;
    }

    try {
      List<String> result = this.getManager().sql(sqlCmd, this.getEngine()).getRows();
      return AggregationResult.newInstance(result, numUnaggregatedFields);

    } catch (Exception e) {
      e.printStackTrace();
      mLog.error(e.getMessage());
      throw new DDFException("Unable to query from " + tableName, e);
    }
  }

  @Override
  public AggregationResult xtabs(List<AggregateField> fields) throws DDFException {
    return this.aggregate(fields);
  }


  @Override
  public double aggregateOnColumn(AggregateFunction function, String column) throws DDFException {
    return Double.parseDouble(this.getManager()
        .sql(String.format("SELECT %s from %s", function.toString(column), this.getDDF().getTableName()), this.getEngine()).getRows().get(0));
  }

  //dplyr-like
  @Override
  public DDF groupBy(List<String> groupedColumns, List<String> aggregateFunctions) throws DDFException {
    mGroupedColumns = groupedColumns;
    return agg(aggregateFunctions);
  }

  //pandas-like
  @Override
  public DDF groupBy(List<String> groupedColumns) {
    mGroupedColumns = groupedColumns;
    return this.getDDF();
  }

  @Override
  public DDF agg(List<String> aggregateFunctions) throws DDFException {

    if (mGroupedColumns.size() > 0) {
      String tableName = this.getDDF().getTableName();

      String groupedColSql = Joiner.on(",").join(mGroupedColumns);

      String selectFuncSql = convertAggregateFunctionsToSql(aggregateFunctions.get(0));
      for (int i = 1; i < aggregateFunctions.size(); i++) {
        selectFuncSql += "," + convertAggregateFunctionsToSql(aggregateFunctions.get(i));
      }

      String sqlCmd = String.format("SELECT %s , %s FROM %s GROUP BY %s", selectFuncSql, groupedColSql, tableName,
          groupedColSql);
      mLog.info("SQL Command: " + sqlCmd);

      try {
        DDF resultDDF = this.getManager().sql2ddf(sqlCmd, this.getEngine());
        return resultDDF;

      } catch (Exception e) {
        e.printStackTrace();
        throw new DDFException("Unable to query from " + tableName, e);
      }

    } else {
      throw new DDFException("Need to set grouped columns before aggregation");
    }
  }

  private String convertAggregateFunctionsToSql(String sql) {

    if (Strings.isNullOrEmpty(sql)) return null;

    String[] splits = sql.trim().split("=(?![^()]*+\\))");
    if (splits.length == 2) {
      return String.format("%s AS %s", splits[1], splits[0]);
    } else if (splits.length == 1) { // no name for aggregated value
      return splits[0];
    }
    return sql;
  }
}
