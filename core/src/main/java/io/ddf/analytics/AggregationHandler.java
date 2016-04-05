package io.ddf.analytics;


import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.types.AggregateTypes.AggregateField;
import io.ddf.types.AggregateTypes.AggregateFunction;
import io.ddf.types.AggregateTypes.AggregationResult;
import io.ddf.util.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class AggregationHandler extends ADDFFunctionalGroupHandler implements IHandleAggregation {

  private List<String> mGroupedColumns;

  public AggregationHandler(DDF theDDF) {
    super(theDDF);
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
   * @return an object of class {@link AggregationResult}
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
      throw new DDFException(e.getMessage(), e);
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

  protected String buildGroupBySQL(List<String> aggregateFunctions) throws DDFException {
    String groupedColSql = Joiner.on(",").join(mGroupedColumns);

    Set<String> aggregatedColumnNames = new HashSet<>();
    List<AggregatedColumnExpression> aggregatedColumnExpressions = new ArrayList<>();
    for (String aggFunc: aggregateFunctions) {
      AggregatedColumnExpression newExp = AggregatedColumnExpression.fromString(aggFunc);
      if (newExp.columnName != null) {
        if (aggregatedColumnNames.contains(newExp.columnName)) {
          throw new DDFException("Duplicated column name in aggregations: " + newExp.columnName);
        }
        if (mGroupedColumns.contains(newExp.columnName)) {
          throw new DDFException("New column name in aggregation cannot be a group by column: " + newExp.columnName);
        }
        aggregatedColumnNames.add(newExp.columnName);
      }
      aggregatedColumnExpressions.add(newExp);
    }

    String selectFuncSql = Joiner.on(", ").join(aggregatedColumnExpressions);

    return String.format("SELECT %s , %s FROM %s GROUP BY %s",
            selectFuncSql,
            groupedColSql,
            "{1}",
            groupedColSql);
  }

  @Override
  public DDF agg(List<String> aggregateFunctions) throws DDFException {

    if (mGroupedColumns.size() > 0) {
      // String tableName = this.getDDF().getTableName();

      String sqlCmd = buildGroupBySQL(aggregateFunctions);

      mLog.info("SQL Command: " + sqlCmd);

      try {
        return this.getManager().sql2ddf(sqlCmd,
                        new SQLDataSourceDescriptor(sqlCmd,
                        null, null,null, this.getDDF().getUUID().toString()));

      } catch (Exception e) {
        e.printStackTrace();
        throw new DDFException(e.getMessage(),
                e);
      }

    } else {
      throw new DDFException("Need to set grouped columns before aggregation");
    }
  }

  private static class AggregatedColumnExpression {
    public final String columnName;
    public final String columnExpression;

    public AggregatedColumnExpression(String columnName, String columnExpression) {
      this.columnName = columnName;
      this.columnExpression = columnExpression;
    }

    public AggregatedColumnExpression(String expression) {
      this(null, expression);
    }

    // TODO: this function need test
    public static AggregatedColumnExpression fromString(String exp) throws DDFException {
      if (Strings.isNullOrEmpty(exp)) {
        throw new DDFException("Aggregation function cannot be null or empty");
      }

      // XXX @nhanitvn what is this regexp doing?
      String[] splits = exp.trim().split("=(?![^()]*+\\))");
      if (splits.length == 2) {
        // A new column name is provided
        String name = splits[0].trim();
        String value = splits[1].trim();
        return new AggregatedColumnExpression(name, value);
      } else if (splits.length == 1) {
        // no new name for aggregated value
        return new AggregatedColumnExpression(splits[0]);
      }
      return new AggregatedColumnExpression(exp);
    }

    @Override
    public String toString() {
      // format as sql expression
      if (columnName != null) {
        return String.format("%s AS %s", columnExpression, columnName);
      }
      return columnExpression;
    }
  }
}
