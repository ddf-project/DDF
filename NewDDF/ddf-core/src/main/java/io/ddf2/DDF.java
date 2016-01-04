package io.ddf2;

import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.handlers.*;

import java.util.HashMap;
import java.util.Map;

public abstract class DDF implements IDDF {
	/*
		Common Properties For DDF
	 */

	/* DataSource keeps all data info of DDF like ISchema, Storage */
	protected IDataSource dataSource;
	/* Num Row Of DDF */
	protected long numRows;
	/* DDF Name */
	protected String ddfName;
	/*Each DDFManager will pass all required Properties to its DDF */
	protected Map mapDDFProperties;
	/*
		Common Handler. Each handler provide subset function for Analytics & Machine Learning
	 */

	protected IAggregationHandler aggregationHandler;
	protected IBinningHandler binningHandler;
	protected IMLHandler mlHandler;
	protected IMLMetricHandler mlMetricHandler;
	protected IStatisticHandler statisticHandler;
	protected ITransformHandler transformHandler;
	protected IViewHandler viewHandler;



	protected DDF(IDataSource dataSource){
		this.dataSource = dataSource;
	}

	/**
	 * Finally build DDF. Called from builder.
	 * @param mapDDFProperties is a contract between concrete DDFManager & concrete DDF
	 */
	protected abstract void build(Map mapDDFProperties);

	/**
	 * @see io.ddf2.IDDF#getDataSource()
	 */
	public IDataSource getDataSource() {
		return dataSource;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getDDFName()
	 */
	public String getDDFName() {
		return ddfName;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getSchema()
	 */
	public ISchema getSchema() {
		return dataSource.getISchema();
	}
	 
	/**
	 * @see io.ddf2.IDDF#getNumColumn()
	 */
	public int getNumColumn() {
		return dataSource.getNumColumn();
	}
	 
	/**
	 * @see io.ddf2.IDDF#sql(java.lang.String)
	 */
	public abstract IDDFResultSet sql(String sql);
	 
	/**
	 * @see io.ddf2.IDDF#getNumRows()
	 */
	public long getNumRows() {
		return numRows >=0 ? numRows : (numRows=_getNumRows());
	}
	/*
		Actually execute compution to get total row.
	 */
	protected abstract long _getNumRows();
	/**
	 * @see io.ddf2.IDDF#getStatisticHandler()
	 */
	public IStatisticHandler getStatisticHandler() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getViewHandler()
	 */
	public IViewHandler getViewHandler() {
		return viewHandler;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getMLHandler()
	 */
	public IMLHandler getMLHandler() {
		return mlHandler;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getMLMetricHandler()
	 */
	public IMLMetricHandler getMLMetricHandler() {
		return mlMetricHandler;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getAggregationHandler()
	 */
	public IAggregationHandler getAggregationHandler() {
		return aggregationHandler;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getBinningHandler()
	 */
	public IBinningHandler getBinningHandler() {
		return binningHandler;
	}
	 
	/**
	 * @see io.ddf2.IDDF#getTransformHandler()
	 */
	public ITransformHandler getTransformHandler() {
		return transformHandler;
	}

	protected abstract IDataSourcePreparer getDataSourcePreparer();




	public static abstract class Builder<T extends DDF>{
		protected T ddf;
		protected Map<String,Object> mapProperties;
		public Builder(IDataSource dataSource){
			ddf = newInstance(dataSource);
			mapProperties = new HashMap<>();
		}
		public Builder(String sqlQuery){
			ddf = newInstance(sqlQuery);
			mapProperties = new HashMap<>();
		}
		protected  abstract T newInstance(IDataSource ds);
		protected  abstract T newInstance(String ds);
		/* Finally Initialize DDF */
		public T build(){
			ddf.build(mapProperties);
			return ddf;
		}
		public Builder<T> setAggregationHandler(IAggregationHandler aggregationHandler) {
			ddf.aggregationHandler = aggregationHandler; return this;
		}

		public Builder<T> setBinningHandler(IBinningHandler binningHandler) {
			ddf.binningHandler = binningHandler; return this;
		}

		

		public Builder<T> setMLMetricHandler(IMLMetricHandler mlMetricHandler) {
			ddf.mlMetricHandler = mlMetricHandler; return this;
		}

		public Builder<T> setMLHandler(IMLHandler mlHandler) {
			ddf.mlHandler = mlHandler; return this;
		}

		

		public Builder<T> setStatisticHandler(IStatisticHandler statisticHandler) {
			ddf.statisticHandler = statisticHandler; return this;
		}



		public Builder<T> setTransformHandler(ITransformHandler transformHandler) {
			ddf.transformHandler = transformHandler; return this;
		}

		public Builder<T> setViewHandler(IViewHandler viewHandler) {
			ddf.viewHandler = viewHandler; return this;
		}

		public Builder<T> putProperty(String key,Object value){
			mapProperties.put(key, value);
			return this;
		}
		public Builder<T> putProperty(Map<String,Object> mapProperties){
			this.mapProperties.putAll(mapProperties);
			return this;
		}
	}
}
 
