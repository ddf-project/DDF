package io.ddf.etl;



import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.content.Schema.ColumnType;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.util.Utils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformationHandler extends ADDFFunctionalGroupHandler implements IHandleTransformations {

  public TransformationHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  @Override public DDF transformScaleMinMax() throws DDFException {
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

  @Override public DDF transformScaleStandard() throws DDFException {
    Summary[] summaryArr = this.getDDF().getSummary();
    List<Column> columns = this.getDDF().getSchema().getColumns();

    // Compose a transformation query
    StringBuffer sqlCmdBuffer = new StringBuffer("SELECT ");

    for (int i = 0; i < columns.size(); i++) {
      Column col = columns.get(i);
      if (!col.isNumeric() || col.getColumnClass() == ColumnClass.FACTOR) {
        sqlCmdBuffer.append(col.getName()).append(" ");
      } else {
        // subtract mean, divide by stdev
        sqlCmdBuffer.append(String
            .format("((%s - %s) / %s) as %s ", col.getName(), summaryArr[i].mean(), summaryArr[i].stdev(),
                col.getName()));
      }
      sqlCmdBuffer.append(",");
    }
    sqlCmdBuffer.setLength(sqlCmdBuffer.length() - 1);
    sqlCmdBuffer.append("FROM ").append(this.getDDF().getTableName());

    DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString(), this.getEngine());
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;

  }

  @Override public DDF transformNativeRserve(String transformExpression) {
    return null;
  }

  @Override public DDF transformNativeRserve(String transformExpression, Boolean inPlace) {
    return null;
  }

  @Override public DDF transformNativeRserve(String[] transformExpressions) {
    return null;
  }

  @Override public DDF transformNativeRserve(String[] transformExpressions, Boolean inPlace) {
    return null;
  }

  @Override public DDF transformPython(String[] transformFuctions, String[] functionNames, String[] destColumns,
      String[][] sourceColumns) throws DDFException {
    return null;
  }

  @Override public DDF transformPython(String[] transformFuctions, String[] functionNames, String[] destColumns,
      String[][] sourceColumns, Boolean inPlace) throws DDFException {
    return null;
  }

  @Override public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
    return null;
  }

  @Override public DDF flattenDDF(String[] columns) {
    return null;
  }

  @Override public DDF flattenDDF() {
    return flattenDDF(null);
  }


  public DDF flattenArrayTypeColumn(String colName) throws DDFException {
    StringBuffer newCols = new StringBuffer();
    Column arrCol = this.getDDF().getColumn(colName);
    if (arrCol.getType() != ColumnType.ARRAY) {
      throw new DDFException("Column to be flattened must be an array");
    }
    String arrSizeStr = this.getDDF().sql(String.format("SELECT size(%s) FROM @this LIMIT 1", colName),
        "Unable to fetch the first value of the requested column").getRows().get(0);

    int arrSize = Integer.parseInt(arrSizeStr);
    for (int i = 0; i < arrSize; i++) {

      newCols.append(String.format(" %s[%d] as %s_c%d,", colName, i, colName, i));
    }

    newCols.setLength(newCols.length() - 1);
    String sqlCmd = String.format("SELECT *, %s FROM @this", newCols.toString());
    DDF newddf = this.getDDF().sql2ddf(sqlCmd);
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  public synchronized DDF transformUDF(String RExprs, List<String> columns) throws DDFException {
    List<String> expressions = Arrays.asList(RExprs);
    return this.transformUDF(expressions, columns);
  }

  public synchronized DDF transformUDF(String RExp) throws DDFException {
    List<String> expressions = Arrays.asList(RExp);
    return this.transformUDF(expressions);
  }

  public synchronized DDF transformUDF(List<String> RExps, List<String> columns) throws DDFException {
    String sqlCmd =
        String.format("SELECT %s FROM %s", RToSqlUdf(RExps, columns, this.getDDF().getSchema().getColumns()), "{1}");

    DDF newddf = this.getManager()
        .sql2ddf(sqlCmd, new SQLDataSourceDescriptor(sqlCmd, null, null, null, this.getDDF().getUUID().toString()));

    if (this.getDDF().isMutable()) {
      return this.getDDF().updateInplace(newddf);
    } else {
      newddf.getMetaDataHandler().copyFactor(this.getDDF());
      return newddf;
    }
  }

  public synchronized DDF transformUDF(List<String> RExps) throws DDFException {
    return transformUDF(RExps, null);
  }

  /**
   * Parse R transform expression to Hive equivalent
   *
   * @param RExps : e.g: "foobar = arrtime - crsarrtime, speed = distance / airtime"
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
    } else {
      for (String c : selectedColumns) {
        udfs.add(c);
      }
    }

    Set<String> newColsInRExp = new HashSet<String>();
    for (String str : RExps) {
      int index = str.indexOf("=") > str.indexOf("~") ? str.indexOf("=") : str.indexOf("~");
      String[] udf = new String[2];
      if (index == -1) {
        udf[0] = str;
      } else {
        udf[0] = str.substring(0, index);
        udf[1] = str.substring(index + 1);
      }

      // String[] udf = str.split("[=~](?![^()]*+\\))");
      String newCol = (index != -1) ? udf[0].trim().replaceAll("\\W", "") : udf[0].trim();
      if (newColsInRExp.contains(newCol)) {
        throw new RuntimeException(String.format(dupColExp, newCol));
      }
      String newDef = (index != -1) ? udf[1].trim() : null;
      if (!udfs.contains(newCol)) {
        udfs.add(newCol);
      } else if (!updateOnConflict) {
        throw new RuntimeException(String.format(dupColExp, newCol));
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


  /**
   * Build the SQL from the list of new columns, transformation expression and selected columns
   * @param newColumnNames new column names for transformation expressions
   * @param transformExpressions transformation expressions
   * @param selectedColumns selected existing columns
   * @return the SQL
   * @throws DDFException
   */
  protected String buildTransformUDFWithNamesSQL(String[] newColumnNames, String[] transformExpressions,
                                                 String[] selectedColumns) throws DDFException {

    List<String> lsExistingColumns = this.getDDF().getSchema().getColumns().stream()
            .map(Column::getName).collect(Collectors.toList());

    // make up new names for entries with null or empty in `newColumnNames`
    List<String> lsNewColumnNamesTrimmed = new ArrayList<>();

    // we prioritize the named entries in newColumnNames
    Set<String> setNamedColumns = Arrays.stream(newColumnNames)
            .filter(s -> s != null && !s.isEmpty() && !s.replaceAll("\\W", "").trim().isEmpty())
            .map(s -> s.replaceAll("\\W", "").trim())
            .collect(Collectors.toSet());

    int iNewColIdx = 0;
    for (String sCol: newColumnNames) {
      String sColTrimmed = null;
      if (sCol != null && !sCol.isEmpty()) {
        sColTrimmed = sCol.replaceAll("\\W", "").trim();
      }

      if (sColTrimmed != null && !sColTrimmed.isEmpty()) {
        if (Utils.containsIgnoreCase(lsExistingColumns, sColTrimmed)
            || Utils.containsIgnoreCase(lsNewColumnNamesTrimmed, sColTrimmed)) {
          throw new DDFException(String.format("Duplicated column name: %s. "
              + "Please use another name for the column.", sColTrimmed));
        }
        lsNewColumnNamesTrimmed.add(sColTrimmed);
      } else {
        String sNewColName = String.format("c%d", iNewColIdx);
        while (Utils.containsIgnoreCase(lsExistingColumns, sNewColName)
                || Utils.containsIgnoreCase(setNamedColumns, sNewColName)
                || Utils.containsIgnoreCase(lsNewColumnNamesTrimmed, sNewColName)) {
          iNewColIdx++;
          sNewColName = String.format("c%d", iNewColIdx);
        }
        lsNewColumnNamesTrimmed.add(sNewColName);
      }
    }

    // added transform expressions first
    List<String> newColumns = new ArrayList<>();
    for (int i = lsNewColumnNamesTrimmed.size() - 1; i >= 0; --i) {
      if (transformExpressions[i] == null || transformExpressions[i].isEmpty()) {
        throw new DDFException(String.format("Got empty transform expression at index %d", i));
      }
      newColumns.add(0, String.format("%s AS %s", transformExpressions[i].trim(), lsNewColumnNamesTrimmed.get(i)));
    }

    // at this point, it's sure that newColumns doesn't contain any duplicated columns
    if (selectedColumns == null || selectedColumns.length == 0) {
      newColumns.addAll(0, lsExistingColumns);
    } else {
      for (String selectedCol: selectedColumns) {
        if (!Utils.containsIgnoreCase(lsExistingColumns, selectedCol)) {
          throw new DDFException(String.format("Selected column '%s' does not exist in the DDF", selectedCol));
        }
        newColumns.add(0, selectedCol);
      }
    }

    // build the SQL command
    StringBuilder sqlCmdBuilder = new StringBuilder("SELECT");

    for (int i = 0; i < newColumns.size(); ++i) {
      sqlCmdBuilder.append(" ").append(newColumns.get(i));
      if (i != newColumns.size() - 1) {
        sqlCmdBuilder.append(",");
      }
    }
    sqlCmdBuilder.append(" FROM {1}");

    return sqlCmdBuilder.toString();

  }

  /**
   * Create new columns or overwrite existing ones
   *
   * @param newColumnNames       Array of new column names.
   *                             Empty or null entries in this array
   *                             will be set to c0, c1, c2, etc..
   * @param transformExpressions array of transform expressions. Has to have the same length
   *                             with newColumnNames.
   * @param selectedColumns      list of column names to be included in the result DDF.
   *                             If null or empty, all existing columns will be included.
   * @return current DDF if it is mutable, or a new DDF otherwise.
   * @throws DDFException
   */
  public synchronized DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns) throws DDFException {

    if (newColumnNames == null || newColumnNames.length == 0 || transformExpressions == null
        || transformExpressions.length == 0) {
      return this.getDDF();
    }

    if (newColumnNames.length != transformExpressions.length) {
      throw new DDFException(String
          .format("newColumnNames and transformExpressions must have the same length. Got %d and %d",
              newColumnNames.length, transformExpressions.length));
    }


    String sqlCmd = buildTransformUDFWithNamesSQL(newColumnNames, transformExpressions, selectedColumns);

    DDF newddf = this.getManager()
        .sql2ddf(sqlCmd, new SQLDataSourceDescriptor(sqlCmd, null, null, null, this.getDDF().getUUID().toString()));
    newddf.getMetaDataHandler().copyFactor(this.getDDF());
    return newddf;
  }

  public DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns, Boolean inPlace) throws DDFException {

    DDF newDDF = transformUDFWithNames(newColumnNames, transformExpressions, selectedColumns);
    if(inPlace) {
      return this.getDDF().updateInplace(newDDF);
    } else {
      return newDDF;
    }
  }

  public static String RToSqlUdf(List<String> RExp) {
    return RToSqlUdf(RExp, null, null);
  }

  public static String RToSqlUdf(String RExp) {
    List<String> RExps = new ArrayList<String>();
    RExps.add(RExp);
    return RToSqlUdf(RExps, null, null);
  }

  @Override
  public DDF sort(List<String> columns, List<Boolean> ascending) throws DDFException {
    return sort(columns, ascending, false);
  }

  @Override
  public DDF sort(List<String> columns, List<Boolean> ascending, Boolean inPlace) throws DDFException {
    if (columns == null || columns.isEmpty()) {
      throw new DDFException("List of columns to sort is empty");
    }
    List<String> ddfColumns = this.getDDF().getSchema().getColumns().stream()
            .map(Column::getName).collect(Collectors.toList());
    for (String col : columns) {
      if (!ddfColumns.contains(col)) {
        throw new DDFException(String.format("Column to sort: %s is not in the DDF", col));
      }
    }

    if (ascending == null) {
      ascending = new ArrayList<>();
    }

    Iterator<String> columnIterator = columns.iterator();
    Iterator<Boolean> ascendingIterator = ascending.iterator();
    StringJoiner joiner = new StringJoiner(",", "", "");
    while (columnIterator.hasNext()) {
      boolean asc = ascendingIterator.hasNext() ? ascendingIterator.next() : true;
      joiner.add(String.format("%s %s", columnIterator.next(), asc ? "asc" : "desc"));
    }

    String cmd = String.format("select * from %s order by ", this.getDDF().getSchema().getTableName()).concat(joiner.toString());
    DDF newDDF = this.getManager().sql2ddf(cmd, this.getEngine());

    newDDF.getMetaDataHandler().copyFactor(this.getDDF());
    if (inPlace) {
      return this.getDDF().updateInplace(newDDF);
    } else {
      return newDDF;
    }
  }

  @Override public DDF factorIndexer(List<String> columns) throws DDFException {
    throw new UnsupportedOperationException();
  }

  @Override public DDF inverseFactorIndexer(List<String> columns) throws DDFException {
    throw new UnsupportedOperationException();
  }

  @Override public DDF oneHotEncoding(String inputColumn, String outputColumnName) throws DDFException {
    throw new UnsupportedOperationException();
  }

  @Override public DDF castType(String column, String newType) throws DDFException {
    throw new UnsupportedOperationException();
  }

  @Override public DDF castType(String column, String newType, Boolean inPlace) throws DDFException {
    if (inPlace) {
      DDF ddf = castType(column, newType);
      return this.getDDF().updateInplace(ddf);
    } else {
      return castType(column, newType);
    }
  }
}
