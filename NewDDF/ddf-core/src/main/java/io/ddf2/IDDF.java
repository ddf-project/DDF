package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.Schema;
import io.ddf2.handlers.*;


/**
 * a DDF have an unique Name, Schema & datasource.
 * a DDF is a table-like abstraction which provide custom function like: execute sql, getHandler
 */
public interface IDDF {
 
	 
	public IDataSource getDataSource();
	public String getDDFName();
	public  Schema getSchema();
	public  int getNumColumn();
	public IDDFResultSet sql(String sql);
	public  long getNumRows();
	public  IStatisticHandler getStatisticHandler();
	public  IViewHandler getViewHandler();
	public  IMLHandler getMLHandler();
	public  IMLMetricHandler getMLMetricHandler();
	public  IAggregationHandler getAggregationHandler();
	public  IBinningHandler getBinningHandler();
	public  ITransformHandler getTransformHandler();

}
 
