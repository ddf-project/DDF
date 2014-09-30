package io.ddf.content;


import java.util.List;
import io.ddf.Factor;
import io.ddf.content.Schema.Column;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;


public interface IHandleSchema extends IHandleDDFFunctionalGroup {

  Schema getSchema();

  void setSchema(Schema schema);

  String getTableName();

  Column getColumn(String columnName);

  List<Column> getColumns();

  int getNumColumns();

  String newTableName();

  /**
   * The name will reflect the class name of the given forObject
   * 
   * @param forObject
   * @return
   */
  String newTableName(Object forObject);

  int getColumnIndex(String columnName);

  String getColumnName(int columnIndex);

  /**
   * Generate a basic schema for the current DDF
   */
  Schema generateSchema() throws DDFException;

  void computeFactorLevelsAndLevelCounts() throws DDFException;

  void setFactorLevelsForStringColumns(String[] xCols) throws DDFException;

  Factor<?> setAsFactor(String columnName);

  Factor<?> setAsFactor(int columnIndex);

  void unsetAsFactor(String columnName);

  void unsetAsFactor(int columnIndex);

  public void generateDummyCoding() throws NumberFormatException, DDFException;
}
