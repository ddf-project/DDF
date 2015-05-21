package io.ddf.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface IHandleTransformations extends IHandleDDFFunctionalGroup {

  public DDF transformScaleMinMax() throws DDFException;

  public DDF transformScaleStandard() throws DDFException;

  public DDF transformNativeRserve(String transformExpression);

  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);

  public DDF transformUDF(String transformExpression, List<String> columns) throws DDFException;

  public DDF flattenDDF(String[] columns) throws DDFException;

  public DDF flattenDDF() throws DDFException;
}
