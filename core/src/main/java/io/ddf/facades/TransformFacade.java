package io.ddf.facades;


import io.ddf.DDF;
import io.ddf.etl.IHandleTransformations;
import io.ddf.exception.DDFException;
import java.util.ArrayList;
import java.util.List;

public class TransformFacade implements IHandleTransformations {
  private DDF mDDF;
  private IHandleTransformations mTransformationHandler;


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
  public DDF transformNativeRserve(String transformExpression, Boolean inPlace) {
    return mTransformationHandler.transformNativeRserve(transformExpression, inPlace);
  }

  @Override
  public DDF transformNativeRserve(String[] transformExpressions) {
    return mTransformationHandler.transformNativeRserve(transformExpressions);
  }

  @Override
  public DDF transformNativeRserve(String[] transformExpressions, Boolean inPlace) {
    return mTransformationHandler.transformNativeRserve(transformExpressions, inPlace);
  }

  @Override
  public DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns) throws DDFException {
    return mTransformationHandler.transformPython(transformFunctions, functionNames, destColumns, sourceColumns);
  }

  @Override
  public DDF transformPython(String[] transformFunctions, String[] functionNames,
      String[] destColumns, String[][] sourceColumns, Boolean inPlace) throws DDFException {
    return mTransformationHandler.transformPython(transformFunctions, functionNames, destColumns,
        sourceColumns, inPlace);
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
  public DDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException {
    return mTransformationHandler.transformUDF(transformExpressions, columns);
  }

  public DDF transformUDF(List<String> transformExpressions) throws DDFException {
    return transformUDF(transformExpressions, null);
  }

  public DDF transformUDF(String transformExpression, List<String> columns) throws DDFException {
    List<String> transformExpressions = new ArrayList<String>();
    transformExpressions.add(transformExpression);
    return mTransformationHandler.transformUDF(transformExpressions, columns);
  }

  /**
   * Create a new column or overwrite an existing one.
   *
   * @param transformExpression
   *          the expression in format column=expression
   * @return a DDF
   * @throws DDFException
   */
  public DDF transformUDF(String transformExpression) throws DDFException {
    return transformUDF(transformExpression, null);
  }

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
  public DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns) throws DDFException {
    return mTransformationHandler.transformUDFWithNames(newColumnNames, transformExpressions, selectedColumns);
  }

  public DDF transformUDFWithNames(String[] newColumnNames, String[] transformExpressions,
      String[] selectedColumns, Boolean inPlace) throws DDFException {
    return mTransformationHandler.transformUDFWithNames(newColumnNames, transformExpressions, selectedColumns, inPlace);
  }

  public DDF flattenDDF(String[] columns) throws DDFException {
    return flattenDDF(columns);
  }

  public DDF flattenDDF() throws DDFException {
    return flattenDDF();
  }

  public DDF flattenArrayTypeColumn(String colName) throws DDFException {
    return mTransformationHandler.flattenArrayTypeColumn(colName);
  }

  public DDF factorIndexer(List<String> columns) throws DDFException {
    return mTransformationHandler.factorIndexer(columns);
  }

  public DDF inverseFactorIndexer(List<String> columns) throws DDFException {
    return mTransformationHandler.inverseFactorIndexer(columns);
  }

  public DDF oneHotEncoding(String inputColumn, String outputColumnName) throws DDFException {
    return mTransformationHandler.oneHotEncoding(inputColumn, outputColumnName);
  }

  @Override
  public DDF sort(List<String> columns, List<Boolean> ascending) throws DDFException {
    return mTransformationHandler.sort(columns, ascending);
  }

  public DDF castType(String column, String newType) throws DDFException {
    return mTransformationHandler.castType(column, newType);
  }

  public DDF castType(String column, String newType, Boolean inPlace) throws DDFException {
    return mTransformationHandler.castType(column, newType, inPlace);
  }
}
