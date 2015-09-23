package io.ddf.etl;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

import java.util.*;

public class TransformationHandler extends ADDFFunctionalGroupHandler implements IHandleTransformations {

  public TransformationHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  @Override
  public DDF transformScaleMinMax() throws DDFException {
    Summary[] summaryArr = this.getDDF().getSummary();
    List<Column> columns = this.getDDF().getSchema().getColumns();

    // Compose a transformation query
    StringBuffer sqlCmdBuffer = new StringBuffer("SELECT ");

    for (int i = 0; i < columns.size(); i++) {
      Column col = columns.get(i);
      if (!col.isNumeric() || col.getColumnClass() == ColumnClass.FACTOR) {
        sqlCmdBuffer.append(col.getName()).append(" ");
      } else {
        // subtract min, divide by (max - min)
        sqlCmdBuffer.append(String.format("((%s - %s) / %s) as %s ", col.getName(), summaryArr[i].min(),
            (summaryArr[i].max() - summaryArr[i].min()), col.getName()));
      }
      sqlCmdBuffer.append(",");
    }
    sqlCmdBuffer.setLength(sqlCmdBuffer.length() - 1);
    sqlCmdBuffer.append("FROM ").append(this.getDDF().getTableName());

    DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString(), this.getEngine());
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  @Override
  public DDF transformScaleStandard() throws DDFException {
    Summary[] summaryArr = this.getDDF().getSummary();
    List<Column> columns = this.getDDF().getSchema().getColumns();

    // Compose a transformation query
    StringBuffer sqlCmdBuffer = new StringBuffer("SELECT ");

    for (int i = 0; i < columns.size(); i++) {
      Column col = columns.get(i);
      if (!col.isNumeric() || col.getColumnClass() == ColumnClass.FACTOR) {
        sqlCmdBuffer.append(col.getName());
      } else {
        // subtract mean, divide by stdev
        sqlCmdBuffer.append(String.format("((%s - %s) / %s) as %s ", col.getName(), summaryArr[i].mean(),
            summaryArr[i].stdev(), col.getName()));
      }
      sqlCmdBuffer.append(",");
    }
    sqlCmdBuffer.setLength(sqlCmdBuffer.length() - 1);
    sqlCmdBuffer.append("FROM ").append(this.getDDF().getTableName());

    DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString(), this.getEngine());
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;

  }

  public DDF transformNativeRserve(String transformExpression) {
    // TODO Auto-generated method stub
    return null;
  }

  public DDF transformPython(String[] transformFuctions, String[] destColumns, String[][] sourceColumns) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF flattenDDF(String[] columns) {
    return null;
  }

  @Override
  public DDF flattenDDF() {
    return flattenDDF(null);
  }

  public DDF transformUDF(String RExprs, List<String> columns)
          throws DDFException {
    List<String> expressions = Arrays.asList(RExprs);
    return this.transformUDF(expressions, columns);
  }

  public DDF transformUDF(String RExp) throws DDFException {
    List<String> expressions = Arrays.asList(RExp);
    return this.transformUDF(expressions);
  }

  public DDF transformUDF(List<String> RExps, List<String> columns) throws DDFException {
    String sqlCmd = String.format("SELECT %s FROM %s",
        RToSqlUdf(RExps, columns, this.getDDF().getSchema().getColumns()),
        "{1}");

    DDF newddf = this.getManager().sql2ddf(sqlCmd, new
            SQLDataSourceDescriptor(sqlCmd, null, null, null, this.getDDF()
            .getUUID().toString()));

    if (this.getDDF().isMutable()) {
      return this.getDDF().updateInplace(newddf);
    } else {
      newddf.getMetaDataHandler().copyFactor(this.getDDF());
      return newddf;
    }
  }

  public DDF transformUDF(List<String> RExps) throws DDFException {
    return transformUDF(RExps, null);
  }

  /**
   * Parse R transform expression to Hive equivalent
   *
   * @param transformExpr : e.g: "foobar = arrtime - crsarrtime, speed = distance / airtime"
   * @return "(arrtime - crsarrtime) as foobar, (distance / airtime) as speed
   */

  public static String RToSqlUdf(List<String> RExps, List<String> selectedColumns, List<Column> existingColumns) {
    List<String> udfs = Lists.newArrayList();
    Map<String, String> newColToDef = new HashMap<String, String>();
    boolean updateOnConflict = (selectedColumns == null || selectedColumns.isEmpty());
    String dupColExp = "%s duplicates another column name";

    if (updateOnConflict) {
      if (existingColumns != null && !existingColumns.isEmpty()) {
        for (Column c : existingColumns) {
          udfs.add(c.getName());
        }
      }
    }
    else {
      for (String c : selectedColumns) {
        udfs.add(c);
      }
    }

    Set<String> newColsInRExp = new HashSet<String>();
    for (String str : RExps) {
      String[] udf = str.split("[=~](?![^()]*+\\))");
      String newCol = (udf.length > 1) ?
        udf[0].trim().replaceAll("\\W", "") :
        udf[0].trim();
      if (newColsInRExp.contains(newCol)) {
        throw new RuntimeException(String.format(dupColExp, newCol));
      }
      String newDef = (udf.length > 1) ? udf[1].trim() : null;
      if (!udfs.contains(newCol)) {
        udfs.add(newCol);
      }
      else if (!updateOnConflict) {
        throw new RuntimeException(String.format(dupColExp,newCol));
      }

      if (newDef != null && !newDef.isEmpty()) {
        newColToDef.put(newCol.replaceAll("\\W", ""), newDef);
      }
      newColsInRExp.add(newCol);
    }

    String selectStr = "";
    for (String udf : udfs) {
      String exp = newColToDef.containsKey(udf) ?
        String.format("%s as %s", newColToDef.get(udf), udf) :
        String.format("%s", udf);
      selectStr += (exp + ",");
    }

    return selectStr.substring(0, selectStr.length() - 1);
  }

  public static String RToSqlUdf(List<String> RExp) {
    return RToSqlUdf(RExp, null, null);
  }

  public static String RToSqlUdf(String RExp) {
    List<String> RExps = new ArrayList<String>();
    RExps.add(RExp);
    return RToSqlUdf(RExps, null, null);
  }
}
