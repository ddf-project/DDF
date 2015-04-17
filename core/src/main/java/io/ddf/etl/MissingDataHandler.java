package io.ddf.etl;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.types.AggregateTypes.AggregateFunction;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handle missing data, based on NA handling methods in Pandas DataFrame
 */
public class MissingDataHandler extends ADDFFunctionalGroupHandler implements IHandleMissingData {

  public MissingDataHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  /**
   * This function filters out rows or columns that contain NA values, Default: axis=0, how='any', thresh=0, columns=null,
   * inplace=false
   *
   * @param axis    = 0: drop by row, 1: drop by column, default 0
   * @param how     = 'any' or 'all', default 'any'
   * @param thresh  = required number of non-NA values to skip, default 0
   * @param columns = only consider NA dropping on the given columns, set to null for all columns of the DDF, default null
   * @return a DDF with NAs filtered
   */
  @Override
  public DDF dropNA(Axis axis, NAChecking how, long thresh, List<String> columns) throws DDFException {
    DDF newddf = null;

    int numcols = this.getDDF().getNumColumns();
    if (columns == null) {
      columns = this.getDDF().getColumnNames();
    }
    String sqlCmd = "";

    if (axis == Axis.ROW) { // drop row with NA
      if (thresh > 0) {
        if (thresh > numcols) {
          throw new DDFException(
              "Required number of non-NA values per row must be less than or equal the number of columns.");
        } else {
          sqlCmd = dropNARowSQL(numcols - thresh + 1, columns);
        }
      } else if (how == NAChecking.ANY) {
        sqlCmd = dropNARowSQL(1, columns);

      } else if (how == NAChecking.ALL) {
        sqlCmd = dropNARowSQL(numcols, columns);
      }

      newddf = this.getManager().sql2ddf(String.format(sqlCmd, this.getDDF().getTableName()));

    } else if (axis == Axis.COLUMN) { // drop column with NA
      List<String> cols = Lists.newArrayList();
      long numrows = this.getDDF().getNumRows();
      if (thresh > 0) {
        if (thresh > numrows) {
          throw new DDFException(
              "Required number of non-NA values per column must be less than or equal the number of rows.");
        } else {
          cols = selectedColumns(numrows - thresh + 1, columns);
        }
      } else if (how == NAChecking.ANY) {

        cols = selectedColumns(1, columns);
      } else if (how == NAChecking.ALL) {
        cols = selectedColumns(numrows, columns);
      }

      newddf = this.getDDF().VIEWS.project(cols);

    } else {
      throw new DDFException(
          "Either choose Axis.ROW for row-based NA filtering or Axis.COLUMN for column-based NA filtering");
    }

    this.getManager().addDDF(newddf);
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  private String dropNARowSQL(long thresh, List<String> columns) {
    StringBuffer caseCmd = new StringBuffer("(");


    for (String col : columns) {

      caseCmd.append(String.format(" (CASE WHEN %s IS NULL THEN 1 ELSE 0 END) +", col));
    }

    caseCmd.setLength(caseCmd.length() - 1); // remove the last "+"
    caseCmd.append(")");

    caseCmd.append(String.format("< %s", thresh));


    String sqlCmd = String.format("SELECT * FROM %%s WHERE %s", caseCmd.toString());
    return sqlCmd;
  }

  private List<String> selectedColumns(long thresh, List<String> columns) throws DDFException {
    List<String> cols = Lists.newArrayList();
    for (String column : columns) {
      if (numberOfNAPerColumn(column) < thresh) {
        cols.add(column);
      }
    }
    return cols;
  }

  private long numberOfNAPerColumn(String column) throws DDFException {
    return Long
        .parseLong(this.getDDF()
            .sql2txt(String.format("SELECT COUNT(*) FROM %%s WHERE %s IS NULL", column), "Unable to run the query.")
            .get(0));
  }


  /**
   * This function fills NA with given values. Default using a scalar value fillNA(value,null, 0, null, null, null,
   * false)
   *
   * @param value           a scalar value to fill all NAs
   * @param method          = 'ffill' for forward fill or 'bfill' for backward fill
   * @param limit           = maximum size gap for forward or backward fill
   * @param function        aggregate function to generate the filled value for a column
   * @param columnsToValues = a map to provide different values to fill for different columns
   * @param columns         = only consider NA filling on the given columns, set to null for all columns of the DDF
   * @param inplace         = false: result in new DDF, true: update on the same DDF
   * @return a DDF with NAs filled
   */
  @Override
  public DDF fillNA(String value, FillMethod method, long limit, AggregateFunction function,
      Map<String, String> columnsToValues,
      List<String> columns) throws DDFException {

    DDF newddf = null;
    if (columns == null) {
      columns = this.getDDF().getColumnNames();
    }

    if (method == null) {
      String sqlCmd = fillNAWithValueSQL(value, function, columnsToValues, columns);
      mLog.info("FillNA sql command: " + sqlCmd);
      newddf = this.getManager().sql2ddf(String.format(sqlCmd, this.getDDF().getTableName()));

    } else { // interpolation methods 'ffill' or 'bfill'
      // TODO:
    }

    this.getManager().addDDF(newddf);
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  private String fillNAWithValueSQL(String value, AggregateFunction function, Map<String, String> columnsToValues,
      List<String> columns) throws DDFException {
    StringBuffer caseCmd = new StringBuffer("");
    for (String col : columns) {
      if (!Strings.isNullOrEmpty(value)) { // fill by value

        if (this.getDDF().getColumn(col).isNumeric()) {
          caseCmd.append(fillNACaseSql(col, value));
        } else {
          caseCmd.append(fillNACaseSql(col, String.format("'%s'", value)));
        }

      } else if (MapUtils.isNotEmpty(columnsToValues)) { // fill different values for different columns
        Set<String> keys = columnsToValues.keySet();

        if (keys.contains(col)) {
          String filledValue = columnsToValues.get(col);
          if (this.getDDF().getColumn(col).isNumeric()) {
            caseCmd.append(fillNACaseSql(col, filledValue));
          } else {
            caseCmd.append(fillNACaseSql(col, String.format("'%s'", filledValue)));
          }
        } else {
          caseCmd.append(String.format("%s,", col));
        }
      } else {// fill by function
        if (function != null) {// fill by function
          Column curColumn = this.getDDF().getColumn(col);
          if (this.getDDF().getColumn(col).isNumeric()) {
            double filledValue = this.getDDF().getAggregationHandler().aggregateOnColumn(function, col);
            if (curColumn.getType() == ColumnType.DOUBLE) {
              caseCmd.append(fillNACaseSql(col, filledValue));
            } else {
              caseCmd.append(fillNACaseSql(col, Math.round(filledValue)));
            }

          } else {
            caseCmd.append(String.format("%s,", col));
          }
        } else {
          throw new DDFException("Unsupported or incorrect aggregate function.");
        }
      }
    }
    caseCmd.setLength(caseCmd.length() - 1); // remove the last "+"
    String sqlCmd = String.format("SELECT %s FROM %%s", caseCmd.toString());
    return sqlCmd;
  }

  private String fillNACaseSql(String column, String filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }

  private String fillNACaseSql(String column, long filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }

  private String fillNACaseSql(String column, double filledValue) {
    return String.format(" (CASE WHEN %s IS NULL THEN %s ELSE %s END) AS %s,", column, filledValue, column, column);
  }


}
