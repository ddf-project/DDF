/**
 *
 */
package io.ddf.content;


import io.ddf.DDF;
import io.ddf.content.ViewHandler.Column;
import io.ddf.content.ViewHandler.Expression;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

/**
 *
 */
public interface IHandleViews extends IHandleDDFFunctionalGroup {

  public DDF sample(long numSamples,
                                  boolean withReplacement,
                                  int seed);

  public DDF sample(double percent, boolean withReplacement, int seed);

  public DDF sampleApprox(double percent, boolean withReplacement, int seed);

  public List<String> head(int numRows) throws DDFException;

  public List<String> top(int numRows, String orderCols, String mode) throws DDFException;

  public DDF project(String... columnNames) throws DDFException;

  public DDF project(List<String> columnNames) throws DDFException;

  public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException;

  public DDF removeColumn(String columnName) throws DDFException;

  public DDF removeColumn(String columnName, Boolean inPlace) throws DDFException;

  public DDF removeColumns(String... columnNames) throws DDFException;


  public DDF removeColumns(List<String> columnNames) throws DDFException;

  public DDF removeColumns(List<String> columnNames, Boolean inPlace) throws DDFException;
}
