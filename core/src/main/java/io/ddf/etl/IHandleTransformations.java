package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;
import java.util.List;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {

  @Deprecated
  DDF transformScaleMinMax() throws DDFException;

  DDF transformScaleMinMax(List<String> columns, Boolean inPlace) throws DDFException;

  @Deprecated
  DDF transformScaleStandard() throws DDFException;

  DDF transformScaleStandard(List<String> columns, Boolean inPlace) throws DDFException;

  @Deprecated
  DDF transformNativeRserve(String transformExpression);

  DDF transformNativeRserve(String transformExpression, Boolean inPlace);

  @Deprecated
  DDF transformNativeRserve(String[] transformExpression);

  DDF transformNativeRserve(String[] transformExpression, Boolean inPlace);

  DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns) throws DDFException;

  DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns, Boolean inPlace) throws DDFException;

  DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);

  /**
   * Create new columns or overwrite existing ones
   *
   * @param transformExpressions
   *          A list of expressions, each is in format of column=expression or expression
   * @param columns
   * @return
   * @throws DDFException
   * @deprecated use {@link #transformUDFWithNames(String[], String[], String[])} instead.
   */
  @Deprecated
  DDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException;

  /**
   * Create new columns or overwrite existing ones
   *
   * @param newColumnNames Array of new column names.
   *                       Empty or null entries in this array
   *                       will be set to c0, c1, c2, etc..
   * @param transformExpressions array of transform expressions. Has to have the same length
   *                             with newColumnNames.
   * @param selectedColumns list of column names to be included in the result DDF.
   *                        If null or empty, all existing columns will be included.
   * @return new DDF
   * @throws DDFException
   */
  DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns) throws DDFException;

  /**
   * Create new columns or overwrite existing ones
   *
   * @param newColumnNames Array of new column names.
   *                       Empty or null entries in this array
   *                       will be set to c0, c1, c2, etc..
   * @param transformExpressions array of transform expressions. Has to have the same length
   *                             with newColumnNames.
   * @param selectedColumns list of column names to be included in the result DDF.
   *                        If null or empty, all existing columns will be included.
   * @param inPlace Whether to return a new DDF or modify the current DDF in place
   * @return current DDF if it is inPlace, or a new DDF otherwise.
   * @throws DDFException
   */
  DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns, Boolean inPlace) throws DDFException;

  @Deprecated
  DDF flattenDDF(String[] columns) throws DDFException;

  @Deprecated
  DDF flattenDDF() throws DDFException;

  @Deprecated
  DDF flattenArrayTypeColumn(String colName) throws DDFException;

  DDF flattenDDF(String[] columns, Boolean inPlace) throws DDFException;

  DDF flattenDDF(Boolean inPlace) throws DDFException;

  DDF flattenArrayTypeColumn(String colName, Boolean inPlace) throws DDFException;

  DDF factorIndexer(List<String> columns) throws DDFException;

  DDF inverseFactorIndexer(List<String> columns) throws DDFException;

  DDF oneHotEncoding(String inputColumn, String outputColumnName) throws DDFException;

  DDF sort(List<String> columns, List<Boolean> ascending) throws DDFException;

  DDF sort(List<String> columns, List<Boolean> ascending, Boolean inPlace) throws DDFException;

  /**
   * Cast a column to newType
   * @param column name of column to be casted
   * @param newType type of new column
   * @return a new DDF
   * @throws DDFException
   */
  DDF castType(String column, String newType) throws DDFException;

  /**
   * Cast a column to newType
   * @param column name of column to be casted
   * @param newType type of new column
   * @param inPlace if true modify the DDF, else return new DDF
   * @return the current DDF if inplace is true, else return new DDF
   * @throws DDFException
   */
  DDF castType(String column, String newType, Boolean inPlace) throws DDFException;
}
