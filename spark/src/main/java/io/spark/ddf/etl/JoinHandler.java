package io.spark.ddf.etl;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.etl.IHandleJoins;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.spark.ddf.util.SparkUtils;
import org.apache.spark.sql.DataFrame;

import java.util.HashSet;
import java.util.List;

public class JoinHandler extends ADDFFunctionalGroupHandler implements IHandleJoins {


  public JoinHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  @Override
  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
      List<String> byRightColumns) throws DDFException {

    String leftTableName = getDDF().getTableName();
    String rightTableName = anotherDDF.getTableName();
    List<Column> rightColumns = anotherDDF.getSchema().getColumns();
    HashSet<String> rightColumNameSet = new HashSet<String>();
    for (Column m : rightColumns) {
      rightColumNameSet.add(m.getName());
    }

    String joinSqlCommand = "SELECT lt.*,%s FROM %s lt %s JOIN %s rt ON (%s)";
    String joinLeftSemiCommand = "SELECT lt.* FROM %s lt %s JOIN %s rt ON (%s)";
    String columnString = "";

    if (byColumns != null && !byColumns.isEmpty()) {
      for (int i = 0; i < byColumns.size(); i++) {
        columnString += String.format("lt.%s = rt.%s AND ", byColumns.get(i), byColumns.get(i));
        rightColumNameSet.remove(byColumns.get(i));
      }
    } else {
      if (byLeftColumns != null && byRightColumns != null && byLeftColumns.size() == byRightColumns.size()
          && !byLeftColumns.isEmpty()) {
        for (int i = 0; i < byLeftColumns.size(); i++) {
          columnString += String.format("lt.%s = rt.%s AND ", byLeftColumns.get(i), byRightColumns.get(i));
          rightColumNameSet.remove(byRightColumns.get(i));
        }
      } else {
        throw new DDFException(String.format("Left and right column specifications are missing or not compatible"),
            null);
      }
    }
    columnString = columnString.substring(0, columnString.length() - 5);

    // we will not select column that is already in left table
    String rightSelectColumns = "";
    for (String name : rightColumNameSet) {
      rightSelectColumns += String.format("rt.%s AS r_%s,", name, name);
    }
    rightSelectColumns = rightSelectColumns.substring(0, rightSelectColumns.length() - 1);

    try {
      if (joinType == JoinType.LEFTSEMI) {
        joinSqlCommand = String.format(joinLeftSemiCommand, leftTableName, joinType.getStringRepr(), rightTableName,
            columnString);
      } else {
        joinSqlCommand = String.format(joinSqlCommand, rightSelectColumns, leftTableName, joinType.getStringRepr(),
            rightTableName, columnString);
      }
    } catch (Exception ex) {
      throw new DDFException(String.format("Error while joinType.getStringRepr()"), null);
    }

    try {
      DDF resultDDF = this.getManager().sql2ddf(joinSqlCommand);
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
    return this.getManager().newDDF(newRDD, new Class<?>[]{DataFrame.class}, null, null, schema);
  }
}
