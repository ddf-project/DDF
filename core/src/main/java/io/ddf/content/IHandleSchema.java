package io.ddf.content;


import io.ddf.Factor;
import io.ddf.content.Schema.Column;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;
import java.util.Map;


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

  Map<String, Integer> computeLevelCounts(String columnName) throws DDFException;

  Map<String, Map<String, Integer>> computeLevelCounts(String[] columnNames) throws DDFException;

  Factor<?> setAsFactor(String columnName) throws DDFException;

  Factor<?> setAsFactor(int columnIndex) throws DDFException;

  void unsetAsFactor(String columnName);

  void unsetAsFactor(int columnIndex);

  void setFactorLevels(String columnName, Factor<?> factor) throws DDFException;

  public void generateDummyCoding() throws NumberFormatException, DDFException;
}
