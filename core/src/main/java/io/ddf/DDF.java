package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.handlers.*;
import io.ddf2.spark.SparkDDF;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class DDF<T extends DDF<T>> {
    /*
    Common Properties For DDF
	 */

  // TODO: @sang (1) Every ddf should have an uri style identifier, which may be generated from name. (2) When
  // should we persist ddf, when should we not persist? Does the persistence behavior relates to the name of ddf?

  /* DataSource keeps all data info of DDF like schema, Storage */
  protected IDataSource dataSource;
  /* Num Row Of DDF */
  protected long numRows;
  /* DDF Name */
  protected String name;
  /* An instance of IDDFManager which create this DDF*/
  protected IDDFManager<T> ddfManager;
  /*Each DDFManager will pass all required Properties to its DDF */
  protected Map mapDDFProperties;
  /*All supported datasource preparer*/
  protected Map<Class, IDataSourcePreparer> mapDataSourcePreparer;

	/*
        Common Handler. Each handler provide subset function for Analytics & Machine Learning
	 */

  protected IAggregationHandler<T> aggregationHandler;
  protected IBinningHandler<T>  binningHandler;
  protected IMLHandler<T> mlHandler;
  protected IMLMetricHandler<T> mlMetricHandler;
  protected IStatisticHandler<T> statisticHandler;
  protected ITransformHandler<T> transformHandler;
  protected IViewHandler<T> viewHandler;
  protected ISchemaHandler<T> schemaHandler;


  protected DDF(IDataSource dataSource) {
    assert dataSource != null;
    this.dataSource = dataSource;
  }

  /**
   * Finally build DDF. Called from builder. It's template pattern + intitProperties + initDataSourcePreparer #
   * resolveDataSource + build()
   *
   * @param mapDDFProperties is a contract between concrete DDFManager & concrete DDF
   */
  protected final void build(Map mapDDFProperties) throws PrepareDataSourceException, UnsupportedDataSourceException {
    this.mapDDFProperties = mapDDFProperties;
    beforeBuild(this.mapDDFProperties);
    // TODO: If so we have to init every datasource preparer in order to use them. Better just init when needed.
    initDSPreparer();

    IDataSourcePreparer preparer = mapDataSourcePreparer.get(dataSource.getClass());
    if (preparer == null)
      throw new UnsupportedDataSourceException(dataSource);
    this.dataSource = preparer.prepare(this.name, dataSource);
    endBuild();
  }


  /***
   * DDFManager will pass ddfProperties to concreted DDF thanks to our contraction. A Concrete-DDF would override this
   * function to init it self before build.
   */
  protected void beforeBuild(Map mapDDFProperties) {
  }

  /**
   * An reserve-function for concrete DDF to hook to build progress.
   */
  protected void endBuild() {
  }


  /***
   * Init @mapDataSourcePreparer. Add all supported DataSource to @mapDataSourcePreparer A DataSourcePreparer will help
   * to prepare a concrete datasource for DDF.
   */
  protected abstract void initDSPreparer();

  public abstract ISqlResult sql(String sql) throws DDFException;

  public abstract ISqlResult sql(String sql, Map<String, String> options) throws DDFException;

  public abstract T sql2ddf(String sql) throws DDFException;

  public abstract T sql2ddf(String sql, Map<String, String> options) throws DDFException;


  /**
   * @see io.ddf2.DDF#getDataSource()
   */
  public IDataSource getDataSource() {
    return dataSource;
  }

  /**
   * @see io.ddf2.DDF#getDDFName()
   */
  public String getDDFName() {
    return name;
  }

  /**
   * @see io.ddf2.DDF#getSchema()
   */
  public ISchema getSchema() {
    return dataSource.getSchema();
  }

  /**
   * @see io.ddf2.DDF#getNumColumn()
   */
  public int getNumColumn() {
    return dataSource.getNumColumn();
  }


  /**
   * @see io.ddf2.DDF#getNumRows()
   */
  public long getNumRows() {
    return numRows >= 0 ? numRows : (numRows = _getNumRows());
  }

  /*
      Actually execute compution to get total row.
   */
  protected abstract long _getNumRows();

  /**
   * @see io.ddf2.DDF#getStatisticHandler()
   */
  public IStatisticHandler<T> getStatisticHandler() {
    return null;
  }

  /**
   * @see io.ddf2.DDF#getViewHandler()
   */
  public IViewHandler<T> getViewHandler() {
    return viewHandler;
  }

  /**
   * @see io.ddf2.DDF#getMLHandler()
   */
  public IMLHandler<T> getMLHandler() {
    return mlHandler;
  }

  /**
   * @see io.ddf2.DDF#getMLMetricHandler()
   */
  public IMLMetricHandler getMLMetricHandler() {
    return mlMetricHandler;
  }

  /**
   * @see io.ddf2.DDF#getAggregationHandler()
   */
  public IAggregationHandler getAggregationHandler() {
    return aggregationHandler;
  }

  /**
   * @see io.ddf2.DDF#getBinningHandler()
   */
  public IBinningHandler getBinningHandler() {
    return binningHandler;
  }

  /**
   * @see io.ddf2.DDF#getTransformHandler()
   */
  public ITransformHandler getTransformHandler() {
    return transformHandler;
  }


  public static abstract class DDFBuilder<T extends DDF> {
    protected T ddf;
    protected Map<String, Object> mapProperties;

    public DDFBuilder(IDataSource dataSource) {
      ddf = newInstance(dataSource);
      mapProperties = new HashMap<>();
    }

    public DDFBuilder(String sqlQuery) {
      ddf = newInstance(sqlQuery);
      mapProperties = new HashMap<>();
    }

    protected abstract T newInstance(IDataSource ds);

    protected abstract T newInstance(String ds);

    /* Finally Initialize DDF */
    public T build() throws DDFException {
      ddf.build(mapProperties);
      return ddf;
    }

    public DDFBuilder<T> setName(String ddfName) {
      ddf.name = ddfName;
      return this;
    }

    public DDFBuilder<T> putProperty(String key, Object value) {
      mapProperties.put(key, value);
      return this;
    }

    public DDFBuilder<T> putProperty(Map<String, Object> mapProperties) {
      this.mapProperties.putAll(mapProperties);
      return this;
    }

    /* DDF Handler */
    public DDFBuilder<T> setAggregationHandler(IAggregationHandler aggregationHandler) {
      ddf.aggregationHandler = aggregationHandler;
      return this;
    }

    public DDFBuilder<T> setBinningHandler(IBinningHandler binningHandler) {
      ddf.binningHandler = binningHandler;
      return this;
    }

    public DDFBuilder<T> setMLMetricHandler(IMLMetricHandler mlMetricHandler) {
      ddf.mlMetricHandler = mlMetricHandler;
      return this;
    }

    public DDFBuilder<T> setMLHandler(IMLHandler mlHandler) {
      ddf.mlHandler = mlHandler;
      return this;
    }


    public DDFBuilder<T> setStatisticHandler(IStatisticHandler statisticHandler) {
      ddf.statisticHandler = statisticHandler;
      return this;
    }


    public DDFBuilder<T> setTransformHandler(ITransformHandler transformHandler) {
      ddf.transformHandler = transformHandler;
      return this;
    }

    public DDFBuilder<T> setViewHandler(IViewHandler viewHandler) {
      ddf.viewHandler = viewHandler;
      return this;
    }

    public DDFBuilder<T> setDDFManager(IDDFManager ddfManager) {
      ddf.ddfManager = ddfManager;
      return this;
    }
  }


  //<editor-fold desc="Handler Adapter>

  //<editor-fold desc="StatisticHandler Api>

  /**
   * @see IStatisticHandler#getSummary()
   */
  public IStatisticHandler.Summary[] getSummary() throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getSummary();
  }

  /**
   * @see IStatisticHandler#getSimpleSummary()
   */
  public IStatisticHandler.SimpleSummary[] getSimpleSummary() throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getSimpleSummary();

  }

  /**
   * @see IStatisticHandler#getFiveNumSummary(List)
   */
  public IStatisticHandler.FiveNumSummary[] getFiveNumSummary(List<String> columns) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getFiveNumSummary(columns);
  }

  /**
   * @see IStatisticHandler#getQuantiles(String, Double[])
   */
  public Double[] getQuantiles(String column, Double[] percentiles) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getQuantiles(column, percentiles);
  }

  /**
   * @see IStatisticHandler#getVariance(String)
   */
  public Double[] getVariance(String column) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getVariance(column);
  }

  /**
   * @see IStatisticHandler#getMean(String)
   */
  public Double getMean(String column) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getMean(column);
  }

  /**
   * @see IStatisticHandler#getCor(String, String)
   */
  public Double getCor(String xColumn, String yColumn) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getCor(xColumn, yColumn);
  }

  /**
   * @see IStatisticHandler#getCovariance(String, String)
   */
  public Double getCovariance(String xColumnName, String yColumnName) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getCovariance(xColumnName, yColumnName);
  }

  /**
   * @see IStatisticHandler#getMin(String)
   */
  public Double getMin(String column) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getMin(column);
  }

  /**
   * @see IStatisticHandler#getMax(String)
   */
  public Double getMax(String column) throws DDFException {
    assertNotNull(statisticHandler);
    return statisticHandler.getMax(column);
  }

  //</editor-fold>
  //<editor-fold desc="ViewHandler API>

  /**
   * @see IViewHandler#getRandomSample(int, boolean)
   */
  public ISqlResult getRandomSample(int numSamples, boolean withReplacement) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.getRandomSample(numSamples, withReplacement);
  }

  /**
   * @see IViewHandler#getRandomSample2(int, boolean)
   */
  public T getRandomSample2(int numSamples, boolean withReplacement) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.getRandomSample2(numSamples, withReplacement);
  }

  /**
   * @see IViewHandler#getRandomSample(double, boolean)
   */
  public T getRandomSample(double percent, boolean withReplacement) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.getRandomSample(percent, withReplacement);
  }

  /**
   * @see IViewHandler#head(int)
   */
  public ISqlResult head(int numRows) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.head(numRows);
  }

  /**
   * @see IViewHandler#top(int, String, boolean)
   */
  public ISqlResult top(int numRows, String orderByCols, boolean isDesc) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.top(numRows, orderByCols, isDesc);
  }

  /**
   * @see IViewHandler#project(String...)
   */
  public T project(String... columns) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.project(columns);
  }

  /**
   * @see IViewHandler#project(List)
   */
  public T project(List<String> columns) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.project(columns);
  }

  /**
   * @see IViewHandler#subset(List, IViewHandler.Expression)
   */
  @Deprecated
  public T subset(List<IViewHandler.Column> columnExpr, IViewHandler.Expression filter) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.subset(columnExpr, filter);
  }

  /**
   * @see IViewHandler#subset(List, String)
   */
  public T subset(List<String> columnExpr, String filter) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.subset(columnExpr, filter);
  }

  /**
   * @see IViewHandler#removeColumn(String)
   */
  public T removeColumn(String column) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.removeColumn(column);
  }

  /**
   * @see IViewHandler#removeColumns(String...)
   */
  public T removeColumns(String... columns) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.removeColumns(columns);
  }

  /**
   * @see IViewHandler#removeColumns(List)
   */
  public T removeColumns(List<String> columns) throws DDFException {
    assertNotNull(viewHandler);
    return viewHandler.removeColumns(columns);
  }

  //</editor-fold>
  //<editor-fold desc="MLHandler API>

  //</editor-fold>
  //<editor-fold desc="MLMetricHandler API>

  //</editor-fold>
  //<editor-fold desc="AggregationHandler API>

  /**
   * @see IAggregationHandler#computeCorrelation(String, String)
   */
  public double computeCorrelation(String columnA, String columnB) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.computeCorrelation(columnA, columnB);
  }


  /**
   * @see IAggregationHandler#aggregate(String)
   */
  public IAggregationHandler.AggregationResult aggregate(String query) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.aggregate(query);
  }

  /**
   * @see IAggregationHandler#aggregate(List)
   */
  @Deprecated
  public IAggregationHandler.AggregationResult aggregate(List<IAggregationHandler.AggregateField> fields) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.aggregate(fields);
  }

  /**
   * @see IAggregationHandler#aggregate(String, IAggregationHandler.AggregateFunction)
   */
  public double aggregate(String column, IAggregationHandler.AggregateFunction function) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.aggregate(column, function);
  }


  /**
   * @see IAggregationHandler#groupBy(List, List)
   */
  public T groupBy(List<String> columns, List<String> functions) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.groupBy(columns, functions);
  }

  /**
   * @see IAggregationHandler#xtabs(List)
   */
  @Deprecated
  public IAggregationHandler.AggregationResult xtabs(List<IAggregationHandler.AggregateField> fields) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.xtabs(fields);
  }

  /**
   * @see IAggregationHandler#xtabs(String)
   */
  public IAggregationHandler.AggregationResult xtabs(String fields) throws DDFException {
    assertNotNull(aggregationHandler);
    return aggregationHandler.xtabs(fields);
  }

  //</editor-fold>
  //<editor-fold desc="BinningHandler API>

  /**
   * @see IBinningHandler#binning(String, String, int, double[], boolean, boolean)
   */
  @Deprecated
  public T binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                   boolean right) throws DDFException {
    assertNotNull(binningHandler);
    return binningHandler.binning(column, binningType, numBins, breaks, includeLowest, right);
  }

  /**
   * @see IBinningHandler#binningCustom(String, double[], boolean, boolean)
   */
  public T binningCustom(String column, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
    assertNotNull(binningHandler);
    return binningHandler.binningCustom(column, breaks, includeLowest, right);
  }

  /**
   * @see IBinningHandler#binningEq(String, int, boolean, boolean)
   */
  public T binningEq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
    assertNotNull(binningHandler);
    return binningHandler.binningEq(column, numBins, includeLowest, right);
  }

  /**
   * @see IBinningHandler#binningEqFreq(String, int, boolean, boolean)
   */
  public T binningEqFreq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
    assertNotNull(binningHandler);
    return binningHandler.binningEqFreq(column, numBins, includeLowest, right);
  }

  //</editor-fold>
  //<editor-fold desc="TransformHandler API>

  /**
   * @see ITransformHandler#transformScaleMinMax()
   */
  public T transformScaleMinMax() throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformScaleMinMax();
  }

  /**
   * @see ITransformHandler#transformScaleStandard()
   */
  public T transformScaleStandard() throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformScaleStandard();
  }

  /**
   * @see ITransformHandler#transformNativeRserve(String)
   */
  public T transformNativeRserve(String transformExpression) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformNativeRserve(transformExpression);
  }

  /**
   * @see ITransformHandler#transformNativeRserve(String[])
   */
  public T transformNativeRserve(String[] transformExpression) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformNativeRserve(transformExpression);
  }

  /**
   * @see ITransformHandler#transformPython(String[], String[], String[], String[][])
   */
  public T transformPython(String[] transformFunctions, String[] functionNames,
                           String[] destColumns, String[][] sourceColumns) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformPython(transformFunctions, functionNames, destColumns, sourceColumns);
  }

  /**
   * @see ITransformHandler#transformMapReduceNative(String, String, boolean)
   */
  public T transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformMapReduceNative(mapFuncDef, reduceFuncDef, mapsideCombine);
  }

  /**
   * @see ITransformHandler#transformUDF(List, List)
   */
  public T transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.transformUDF(transformExpressions, columns);
  }

  /**
   * @see ITransformHandler#flattenDDF(String[])
   */
  public T flattenDDF(String[] columns) throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.flattenDDF(columns);
  }

  /**
   * @see ITransformHandler#flattenDDF()
   */
  public T flattenDDF() throws DDFException {
    assertNotNull(transformHandler);
    return transformHandler.flattenDDF();
  }

  //</editor-fold>


  private void assertNotNull(IDDFHandler handler) throws DDFException {
    if (handler == null) throw new DDFException("Function Not Supported");
  }

  //</editor-fold>
}
 