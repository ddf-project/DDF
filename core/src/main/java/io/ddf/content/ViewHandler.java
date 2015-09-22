/**
 *
 */
package io.ddf.content;


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import scala.Int;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ViewHandler extends ADDFFunctionalGroupHandler implements IHandleViews {

  public ViewHandler(DDF theDDF) {
    super(theDDF);
  }

  // @SuppressWarnings("unchecked")
  // @Override
  // public <T> Iterator<T> getRowIterator(Class<T> dataType) {
  // if (dataType == null) dataType = (Class<T>) this.getDDF().getRepresentationHandler().getDefaultDataType();
  //
  // Object repr = this.getDDF().getRepresentationHandler().get(dataType);
  // return (repr instanceof Iterable<?>) ? ((Iterable<T>) repr).iterator() : null;
  // }

  @Override
  public List<Object[]> getRandomSample(int numSamples, boolean withReplacement, int seed) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DDF getRandomSampleByNum(int numSamples, boolean withReplacement,
                                  int seed) {
    return null;
  }

  @Override
  public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public List<String> head(int numRows) throws DDFException {
    return this.getDDF().sql(String.format("SELECT * FROM @this LIMIT %d", numRows),
        String.format("Unable to fetch %d row(s) from table %%s", numRows)).getRows();
  }

  public List<String> top(int numRows, String orderColumns, String mode) throws DDFException {

    DDF temp = sql2ddf(String.format("SELECT * FROM %%s order by %s %s", orderColumns, mode),
        String.format("Unable to fetch %d row(s) from table %%s", numRows));

    return (temp.VIEWS.head(numRows));
  }

  @Override
  public DDF project(String... columnNames) throws DDFException {
    if (columnNames == null || columnNames.length == 0) throw new DDFException("columnNames must be specified");

    String selectedColumns = Joiner.on(",").join(columnNames);
    return sql2ddf(String.format("SELECT %s FROM %%s", selectedColumns),
        String.format("Unable to project column(s) %s from table %%s", selectedColumns));
  }

  @Override
  public DDF project(List<String> columnNames) throws DDFException {
    if (columnNames == null || columnNames.isEmpty()) throw new DDFException("columnNames must be specified");

    String selectedColumns = Joiner.on(",").join(columnNames);
    return sql2ddf(String.format("SELECT %s FROM %%s", selectedColumns),
        String.format("Unable to project column(s) %s from table %%s", selectedColumns));
  }

  @Override
  public DDF removeColumn(String columnName) throws DDFException {
    List<String> columns = Lists.newArrayList();

    Collections.addAll(columns, columnName);

    return this.removeColumns(columns);
  }

  @Override
  public DDF removeColumns(String... columnNames) throws DDFException {
    if (columnNames == null || columnNames.length == 0) throw new DDFException("columnNames must be specified");

    List<String> columns = Lists.newArrayList();

    Collections.addAll(columns, columnNames);

    return this.removeColumns(columns);
  }

  @Override
  public DDF removeColumns(List<String> columnNames) throws DDFException {
    if (columnNames == null || columnNames.isEmpty()) throw new DDFException("columnNames must be specified");
    List<String> currentColumnNames = this.getDDF().getColumnNames();
    for(String columnName: columnNames) {
      if(!currentColumnNames.contains(columnName)) {
        throw new DDFException(String.format("Column %s does not exists", columnName));
      }
    }
    List<String> columns = this.getDDF().getColumnNames();

    for (String columnName : columnNames) {
      for (Iterator<String> it = columns.iterator();it.hasNext();) {
        if (it.next().equals(columnName)) {
          it.remove();
        }
      }
    }

    DDF newddf = this.project(columns);
    if(this.getDDF().isMutable()) {
      this.getDDF().updateInplace(newddf);
      return this.getDDF();
    } else {
      newddf.getMetaDataHandler().copyFactor(this.getDDF());
      return newddf;
    }
  }

  // ///// Execute SQL command on the DDF ///////

  private DDF sql2ddf(String sqlCommand, String errorMessage) throws DDFException {
    try {
      return this.getManager().sql2ddf(String.format(sqlCommand, this.getDDF().getTableName()), this.getEngine());

    } catch (Exception e) {
      throw new DDFException(String.format(errorMessage, this.getDDF().getTableName()), e);
    }
  }

  @Override
  public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException {
    updateVectorName(filter, this.getDDF());
    mLog.info("Updated filter: " + filter);

    String[] colNames = new String[columnExpr.size()];
    for (int i = 0; i < columnExpr.size(); i++) {
      updateVectorName(columnExpr.get(i), this.getDDF());
      colNames[i] = columnExpr.get(i).getName();
    }
    mLog.info("Updated columns: " + Arrays.toString(columnExpr.toArray()));

    String sqlCmd = String.format("SELECT %s FROM %s", Joiner.on(", ").join
            (colNames), "{1}");
    if (filter != null) {
      sqlCmd = String.format("%s WHERE %s", sqlCmd, filter.toSql());
    }
    mLog.info("sql = {}", sqlCmd);

    DDF subset = this.getManager().sql2ddf(sqlCmd, new
            SQLDataSourceDescriptor(sqlCmd, null, null, null, this.getDDF()
            .getUUID().toString()));

    subset.getMetaDataHandler().copyFactor(this.getDDF());
    return subset;

  }


  /**
   * Base class for any Expression node in the AST, could be either an Operator or a Value
   */
  static public class Expression implements Serializable {
    String type;


    public String getType() {
      return type;
    }

    public String toSql() {
      return null;
    }

    public void setType(String aType) {
      type = aType;
    }
  }


  public enum OperationName {
    lt, le, eq, ge, gt, ne, and, or, neg, isnull, isnotnull, grep, grep_ic
  }


  /**
   * Base class for unary operations and binary operations
   */
  static public class Operator extends Expression {
    OperationName name;
    Expression[] operands;

    public Operator() {
      super.setType("Operator");
    }

    public OperationName getName() {
      return name;
    }


    public Expression[] getOperands() {
      return operands;
    }

    public void setOperarands(Expression[] ops) {
      this.operands = ops;
    }

    public void setName(OperationName name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "Operator [name=" + name + ", operands=" + Arrays.toString(operands) + "]";
    }

    @Override
    public String toSql() {
      if (name == null) {
        throw new IllegalArgumentException("Missing Operator name from Adatao client for operands[] "
            + Arrays.toString(operands));
      }
      switch (name) {
        case gt:
          return String.format("(%s > %s)", operands[0].toSql(), operands[1].toSql());
        case lt:
          return String.format("(%s < %s)", operands[0].toSql(), operands[1].toSql());
        case ge:
          return String.format("(%s >= %s)", operands[0].toSql(), operands[1].toSql());
        case le:
          return String.format("(%s <= %s)", operands[0].toSql(), operands[1].toSql());
        case eq:
          return String.format("(%s = %s)", operands[0].toSql(), operands[1].toSql());
        case ne:
          return String.format("(%s != %s)", operands[0].toSql(), operands[1].toSql());
        case and:
          return String.format("(%s AND %s)", operands[0].toSql(), operands[1].toSql());
        case or:
          return String.format("(%s OR %s)", operands[0].toSql(), operands[1].toSql());
        case neg:
          return String.format("(NOT %s)", operands[0].toSql());
        case isnull:
          return String.format("(%s IS NULL)", operands[0].toSql());
        case isnotnull:
          return String.format("(%s IS NOT NULL)", operands[0].toSql());
        case grep:
          return String.format("(%s LIKE '%%%s%%')", operands[1].toSql(), operands[0].toSql().substring(1,operands[0].toSql().length()-1));
        case grep_ic:
          return String.format("(lower(%s) LIKE '%%%s%%')", operands[1].toSql(), operands[0].toSql().substring(1,operands[0].toSql().length()-1).toLowerCase());
        default:
          throw new IllegalArgumentException("Unsupported Operator: " + name);
      }
    }
  }


  public abstract static class Value extends Expression {
    public abstract Object getValue();
    public void setType(String type) {
      super.setType(type);
    }
  }


  static public class IntVal extends Value {
    int value;

    public IntVal() {
      super.setType("IntVal");
    }


    @Override
    public String toString() {
      return "IntVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Integer.toString(value);
    }
  }


  static public class DoubleVal extends Value {
    double value;

    public DoubleVal() {
      super.setType("DoubleVal");
    }


    @Override
    public String toString() {
      return "DoubleVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Double.toString(value);
    }
  }


  static public class StringVal extends Value {
    String value;

    public StringVal() {
      super.setType("StringVal");
    }

    public void setValue(String val) {
      this.value = val;
    }

    @Override
    public String toString() {
      return "StringVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return String.format("'%s'", value);
    }
  }


  static public class BooleanVal extends Value {
    Boolean value;

    public BooleanVal() {
      super.setType("BooleanVal");
    }


    @Override
    public String toString() {
      return "BooleanVal [value=" + value + "]";
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toSql() {
      return Boolean.toString(value);
    }
  }


  static public class Column extends Expression {
    String id;
    String name;
    Integer index = null;

    public Column() {
      super.setType("Column");
    }


    public String getID() {
      return id;
    }

    public Integer getIndex() {
      return index;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setIndex(Integer index) {
      this.index = index;
    }

    public void setID(String id) {
      this.id = id;
    }

    public Object getValue(Object[] xs) {
      return xs[index];
    }

    @Override
    public String toSql() {
      assert this.name != null;
      return this.name;
    }

    @Override
    public String toString() {
      return "Column [id=" + id + ", name=" + name + ", index=" + index + "]";
    }

    public String getName() {
      return name;
    }
  }


  private io.ddf.content.Schema.Column[] selectColumnMetaInfo(List<Column> columns, DDF ddf) {
    int length = columns.size();
    io.ddf.content.Schema.Column[] retObj = new io.ddf.content.Schema.Column[length];
    for (int i = 0; i < length; i++) {
      retObj[i] = ddf.getSchema().getColumn(columns.get(i).getIndex());
    }
    return retObj;
  }

  private void updateVectorIndex(Expression expression, DDF ddf) {
    if (expression == null) {
      return;
    }
    if (expression.getType().equals("Column")) {
      Column vec = (Column) expression;
      if (vec.getIndex() == null) {
        String name = vec.getName();
        if (name != null) {
          vec.setIndex(ddf.getColumnIndex(name));
        }
      }
      return;
    }
    if (expression instanceof Operator) {
      Expression[] newOps = ((Operator) expression).getOperands();
      for (Expression newOp : newOps) {
        updateVectorIndex(newOp, ddf);
      }
    }
  }

  private void updateVectorName(Expression expression, DDF ddf) {
    if (expression == null) {
      return;
    }
    if (expression.getType().equals("Column")) {
      Column vec = (Column) expression;
      if (vec.getName() == null) {
        Integer i = vec.getIndex();
        if (i != null) {
          vec.setName(ddf.getSchema().getColumnName(i));
        }
      }
      return;
    }
    if (expression instanceof Operator) {
      Expression[] newOps = ((Operator) expression).getOperands();
      for (Expression newOp : newOps) {
        updateVectorName(newOp, ddf);
      }
    }
  }

}
