package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {

  public DDF transformScaleMinMax() throws DDFException;

  public DDF transformScaleStandard() throws DDFException;

  public DDF transformNativeRserve(String transformExpression);

  public DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns);

  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);

  /**
   * Create new columns or overwrite existing ones
   *
   * @param transformExpressions A list of expressions, each is in format of column=expression or expression
   * @param columns
   * @return
   * @throws DDFException
   */
  public DDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException;

  public DDF flattenDDF(String[] columns) throws DDFException;

  public DDF flattenDDF() throws DDFException;
}
