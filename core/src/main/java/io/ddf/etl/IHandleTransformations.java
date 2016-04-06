package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;
import java.util.List;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {

  DDF transformScaleMinMax() throws DDFException;

  DDF transformScaleStandard() throws DDFException;

  DDF transformNativeRserve(String transformExpression);

  DDF transformNativeRserve(String[] transformExpression);

  DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns);

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
   * @param transformExpressions array of transform expressions. Has to have the same length
   *                             with newColumnNames.
   * @param selectedColumns list of column names to be included in the result DDF.
   *                        If null or empty, all existing columns will be included.
   * @return current DDF if it is mutable, or a new DDF otherwise.
   * @throws DDFException
   */
  DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns) throws DDFException;

  DDF flattenDDF(String[] columns) throws DDFException;

  DDF flattenDDF() throws DDFException;

  DDF flattenArrayTypeColumn(String colName) throws DDFException;

  DDF factorIndexer(List<String> columns) throws DDFException;

  DDF inverseFactorIndexer(List<String> columns) throws DDFException;
}
