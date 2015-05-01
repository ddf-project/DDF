package io.ddf.etl;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import java.util.List;
import java.util.ArrayList;

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

    DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString());
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

    DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString());
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;

  }

  public DDF transformNativeRserve(String transformExpression) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
    // TODO Auto-generated method stub
    return null;
  }

  public DDF transformUDF(String RExp, List<String> columns) throws DDFException {

    String columnList;
    if (columns != null) {
      columnList = Joiner.on(",").skipNulls().join(columns);
    } else {
      columns = new ArrayList<String>();
      for (Column c : this.getDDF().getSchema().getColumns()) {
        columns.add(c.getName());
      }
      columnList = "*";
    }
    String sqlCmd = String.format("SELECT %s, %s FROM %s", columnList, RToSqlUdf(RExp, columns), this.getDDF().getTableName());
    DDF newddf = this.getManager().sql2ddf(sqlCmd);

    if (this.getDDF().isMutable()) {
      return this.getDDF().updateInplace(newddf);
    } else {
      newddf.getMetaDataHandler().copyFactor(this.getDDF());
      return newddf;
    }
  }

  public DDF transformUDF(String RExp) throws DDFException {
    return transformUDF(RExp, null);
  }

  /**
   * Parse R transform expression to Hive equivalent
   *
   * @param transformExpr : e.g: "foobar = arrtime - crsarrtime, speed = distance / airtime"
   * @return "(arrtime - crsarrtime) as foobar, (distance / airtime) as speed
   */

  public static String RToSqlUdf(String RExp, List<String> existingColumns) {
    System.out.println("Existing columns: " + Joiner.on(",").skipNulls().join(existingColumns));
    List<String> udfs = Lists.newArrayList();
    for (String str : RExp.split(",(?![^()]*+\\))")) {
      String[] udf = str.split("[=~](?![^()]*+\\))");
      if (udf.length == 1) {
        udfs.add(String.format("(%s)", udf[0]).trim());
      } else {
        String newCol = udf[0].trim().replaceAll("\\W", "");
        if (existingColumns != null && existingColumns.contains(newCol)) {
            throw new RuntimeException(newCol + " duplicates another column name");
        }
        else {
            udfs.add(String.format("(%s) as %s", udf[1].trim(), newCol));
        }
      }
    }
    return Joiner.on(",").join(udfs);
  }

  public static String RToSqlUdf(String RExp) {
    return RToSqlUdf(RExp, null);
  }
}
