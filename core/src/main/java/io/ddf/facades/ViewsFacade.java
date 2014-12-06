package io.ddf.facades;


import io.ddf.DDF;
import io.ddf.content.IHandleViews;
import io.ddf.content.ViewHandler.Column;
import io.ddf.content.ViewHandler.Expression;
import io.ddf.exception.DDFException;

import java.util.List;

public class ViewsFacade implements IHandleViews {
  private DDF mDDF;
  private IHandleViews mViewHandler;


  public ViewsFacade(DDF ddf, IHandleViews viewHandler) {
    mDDF = ddf;
    mViewHandler = viewHandler;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public IHandleViews getViewHandler() {
    return mViewHandler;
  }

  public void setViewHandler(IHandleViews viewHandler) {
    mViewHandler = viewHandler;
  }

  @Override
  public List<Object[]> getRandomSample(int numSamples, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(numSamples, withReplacement, seed);
  }

  @Override
  public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(percent, withReplacement, seed);
  }

  @Override
  public List<String> head(int numRows) throws DDFException {
    return mViewHandler.head(numRows);
  }

  @Override
  public List<String> top(int numRows, String orderedCols, String mode) throws DDFException {
    return mViewHandler.top(numRows, orderedCols, mode);
  }

  public List<Object[]> getRandomSample(int numSamples) {
    return getRandomSample(numSamples, false, 1);
  }

  @Override
  public DDF project(String... columnNames) throws DDFException {
    return mViewHandler.project(columnNames);
  }

  @Override
  public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException {
    return mViewHandler.subset(columnExpr, filter);
  }

  @Override
  public DDF project(List<String> columnNames) throws DDFException {
    return this.getViewHandler().project(columnNames);
  }

  @Override
  public DDF removeColumn(String columnName) throws DDFException {
    return this.getViewHandler().removeColumn(columnName);
  }

  @Override
  public DDF removeColumns(String... columnNames) throws DDFException {
    return this.getViewHandler().removeColumns(columnNames);
  }

  @Override
  public DDF removeColumns(List<String> columnNames) throws DDFException {
    return this.getViewHandler().removeColumns(columnNames);
  }
}
