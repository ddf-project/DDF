package io.ddf.facades;


import io.ddf.DDF;
import io.ddf.etl.IHandleTransformations;
import io.ddf.exception.DDFException;

import java.util.List;

public class TransformFacade implements IHandleTransformations {
  private DDF mDDF;
  private IHandleTransformations mTransformationHandler;

  {

  }

  public TransformFacade(DDF ddf, IHandleTransformations transformationHandler) {
    this.mDDF = ddf;
    this.mTransformationHandler = transformationHandler;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public IHandleTransformations getmTransformationHandler() {
    return mTransformationHandler;
  }

  public void setmTransformationHandler(IHandleTransformations mTransformationHandler) {
    this.mTransformationHandler = mTransformationHandler;
  }

  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef) {
    return transformMapReduceNative(mapFuncDef, reduceFuncDef, true);

  }

  @Override
  public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
    return mTransformationHandler.transformMapReduceNative(mapFuncDef, reduceFuncDef, mapsideCombine);

  }

  @Override
  public DDF transformNativeRserve(String transformExpression) {
    return mTransformationHandler.transformNativeRserve(transformExpression);

  }

  @Override
  public DDF transformScaleMinMax() throws DDFException {
    return mTransformationHandler.transformScaleMinMax();
  }

  @Override
  public DDF transformScaleStandard() throws DDFException {
    return mTransformationHandler.transformScaleStandard();
  }

  @Override
  public DDF transformUDF(String transformExpression, List<String> columns) throws DDFException {
    return mTransformationHandler.transformUDF(transformExpression, columns);
  }


  public DDF transformUDF(String transformExpression) throws DDFException {
    return transformUDF(transformExpression, null);
  }

  public DDF flattenDDF(List<String> columns) throws DDFException {
    return flattenDDF(columns);
  }

  public DDF flattenDDF() throws DDFException {
    return flattenDDF();
  }
}
