package io.ddf.spark.etl;


import com.google.common.annotations.VisibleForTesting;
import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.etl.IHandleJoins;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.spark.util.SparkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinHandler extends ADDFFunctionalGroupHandler implements IHandleJoins {


  public JoinHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  @Override
  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
                  List<String> byRightColumns) throws DDFException {
    return join(anotherDDF, joinType, byColumns, byLeftColumns, byRightColumns, null, null);
  }

  @Override
  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
      List<String> byRightColumns, String leftSuffix, String rightSuffix) throws DDFException {

    String leftTableName = getDDF().getTableName();
    String rightTableName = anotherDDF.getTableName();
    List<String> rightColumns = anotherDDF.getColumnNames();
    List<String> leftColumns = getDDF().getColumnNames();
    
    String joinSqlCommand = "SELECT %s,%s FROM %s lt %s JOIN %s rt ON (%s)";
    String joinLeftSemiCommand = "SELECT lt.* FROM %s lt %s JOIN %s rt ON (%s)";
    String joinConditionString = "";

    if (byColumns != null && !byColumns.isEmpty()) {
      for (String col: byColumns) {
        joinConditionString += String.format("lt.%s = rt.%s AND ", col, col);
      }
    } else {
      if (byLeftColumns != null && byRightColumns != null && byLeftColumns.size() == byRightColumns.size()
          && !byLeftColumns.isEmpty()) {
        Iterator<String> leftColumnIterator = byLeftColumns.iterator();
        Iterator<String> rightColumnIterator = byRightColumns.iterator();
        while (leftColumnIterator.hasNext()) {
          joinConditionString += String.format("lt.%s = rt.%s AND ", leftColumnIterator.next(), rightColumnIterator.next());
        }
      } else {
        throw new DDFException(String.format("Left and right column specifications are missing or not compatible"),
            null);
      }
    }
    joinConditionString = joinConditionString.substring(0, joinConditionString.length() - 5); //remove " AND " at the end

    // Add suffix to overlapping columns, use default if not provided
    if (leftSuffix == null || StringUtils.isEmpty(leftSuffix.trim())) {
      leftSuffix = "_l";
    }
    if (rightSuffix == null || StringUtils.isEmpty(rightSuffix.trim())) {
      rightSuffix = "_r";
    }

    String leftSelectColumns = generateSelectColumns(leftColumns, rightColumns, "lt", leftSuffix);
    String rightSelectColumns = generateSelectColumns(rightColumns, leftColumns, "rt", rightSuffix);

    try {
      if (joinType == JoinType.LEFTSEMI) {
        joinSqlCommand = String.format(joinLeftSemiCommand, leftTableName, joinType.getStringRepr(), rightTableName,
            joinConditionString);
      } else {
        joinSqlCommand = String.format(joinSqlCommand, leftSelectColumns, rightSelectColumns, leftTableName, joinType.getStringRepr(),
            rightTableName, joinConditionString);
      }
    } catch (Exception ex) {
      throw new DDFException(String.format("Error while joinType.getStringRepr()"), null);
    }

    mLog.info("Join SQL command: " +joinSqlCommand);
    try {
      DDF resultDDF = this.getManager().sql2ddf(joinSqlCommand, "SparkSQL");
      return resultDDF;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DDFException(String.format("Error while executing query QueryExecutionException"), e);
    }

  }
  @Override
  public DDF merge(DDF anotherDDF) throws DDFException {
    DataFrame rdd1 = ((DataFrame) this.getDDF().getRepresentationHandler().get(DataFrame.class));
    DataFrame rdd2 = ((DataFrame) anotherDDF.getRepresentationHandler().get(DataFrame.class));
    DataFrame newRDD = rdd1.unionAll(rdd2);
    Schema schema = SparkUtils.schemaFromDataFrame(newRDD);
    return this.getManager().newDDF(newRDD, new Class<?>[]{DataFrame.class},
             null, null, schema);
  }

  @VisibleForTesting
  public String generateSelectColumns(List<String> targetColumns, List<String> filterColumns, String columnId, String suffix) {
    String selectColumns = "";

    if (targetColumns == null) {
      return selectColumns;
    }
    if (filterColumns == null) {
      filterColumns = new ArrayList<>();
    }

    for (String colName : targetColumns) {
      if (filterColumns.contains(colName)) {
        selectColumns += String.format("%s.%s AS %s%s,", columnId, colName, colName, suffix);
      } else {
        selectColumns += String.format("%s.%s,", columnId, colName);
      }

    }
    if (selectColumns.length() > 0) {
      selectColumns = selectColumns.substring(0, selectColumns.length() - 1); // remove "," at the end
    }

    return selectColumns;
  }
}
