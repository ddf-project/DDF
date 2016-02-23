package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.handlers.*;
import io.ddf2.handlers.IStatisticHandler.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


/**
 * a DDF have an unique Name, schema & datasource.
 * a DDF is a table-like abstraction which provide custom function via its handler.
 */
public interface IDDF {


    public IDataSource getDataSource();

    public String getDDFName();

    public ISchema getSchema();

    public int getNumColumn();

    public ISqlResult sql(String sql) throws DDFException;

    public ISqlResult sql(String sql, Map<String, String> options) throws DDFException;

    public IDDF sql2ddf(String sql) throws DDFException;

    public IDDF sql2ddf(String sql, Map<String, String> options) throws DDFException;

    public long getNumRows();
    @Deprecated
    public IStatisticHandler getStatisticHandler();

    @Deprecated
    public IViewHandler getViewHandler();

    @Deprecated
    public IMLHandler getMLHandler();

    @Deprecated
    public IMLMetricHandler getMLMetricHandler();

    @Deprecated
    public IAggregationHandler getAggregationHandler();

    @Deprecated
    public IBinningHandler getBinningHandler();

    @Deprecated
    public ITransformHandler getTransformHandler();

    //TODO @jing please check transform function
    // [?] Should we change function name
    //<editor-fold desc="StatisticHandler Api">

    /**
     * @see IStatisticHandler#getSummary()
     */
    Summary[] getSummary() throws DDFException;

    /**
     * @see IStatisticHandler#getSimpleSummary()
     */
    SimpleSummary[] getSimpleSummary() throws DDFException;

    /**
     * @see IStatisticHandler#getFiveNumSummary(List)
     */
    FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

    /**
     * @see IStatisticHandler#getQuantiles(String, Double[])
     */
    Double[] getQuantiles(String columnName, Double[] percentiles) throws DDFException;

    /**
     * @see IStatisticHandler#getVariance(String)
     */
    Double[] getVariance(String columnName) throws DDFException;

    /**
     * @see IStatisticHandler#getMean(String)
     */
    Double getMean(String columnName) throws DDFException;

    /**
     * @see IStatisticHandler#getCor(String, String)
     */
    Double getCor(String xColumnName, String yColumnName) throws DDFException;

    /**
     * @see IStatisticHandler#getCovariance(String, String)
     */
    Double getCovariance(String xColumnName, String yColumnName) throws DDFException;

    /**
     * @see IStatisticHandler#getMin(String)
     */
    Double getMin(String columnName) throws DDFException;

    /**
     * @see IStatisticHandler#getMax(String)
     */
    Double getMax(String columnName) throws DDFException;

    //</editor-fold>
    //<editor-fold desc="ViewHandler API>
    /**
     * @see IViewHandler#getRandomSample(int, boolean)
     * */
    ISqlResult getRandomSample(int numSamples, boolean withReplacement) throws DDFException;

    /**
     * @see IViewHandler#getRandomSample2(int, boolean)
     */
    IDDF getRandomSample2(int numSamples, boolean withReplacement) throws DDFException;

    /**
     * @see IViewHandler#getRandomSample(double, boolean)
     */
    IDDF getRandomSample(double percent, boolean withReplacement) throws DDFException;

    /**
     * @see IViewHandler#head(int)
     */
    ISqlResult head(int numRows) throws DDFException;

    /**
     * @see IViewHandler#top(int, String, boolean)
     */
    ISqlResult top(int numRows, String orderByCols, boolean isDesc) throws DDFException;

    /**
     * @see IViewHandler#project(String...)
     */
    IDDF project(String... columnNames) throws DDFException;

    /**
     * @see IViewHandler#project(List)
     */
    IDDF project(List<String> columnNames) throws DDFException;

    /**
     * @see IViewHandler#subset(List, IViewHandler.Expression)
     */
    @Deprecated
    IDDF subset(List<IViewHandler.Column> columnExpr, IViewHandler.Expression filter) throws DDFException;

    /**
     * @see IViewHandler#subset(List, String)
     */
    IDDF subset(List<String> columnExpr, String filter) throws DDFException;

    /**
     * @see IViewHandler#removeColumn(String)
     */
    IDDF removeColumn(String columnName) throws DDFException;

    /**
     * @see IViewHandler#removeColumns(String...)
     */
    IDDF removeColumns(String... columnNames) throws DDFException;

    /**
     * @see IViewHandler#removeColumns(List)
     */
    IDDF removeColumns(List<String> columnNames) throws DDFException;

    //</editor-fold>
    //<editor-fold desc="MLHandler API>

    //</editor-fold>
    //<editor-fold desc="MLMetricHandler API>

    //</editor-fold>
    //<editor-fold desc="AggregationHandler API>
    /**
     * @see IAggregationHandler#computeCorrelation(String, String)
     */
    double computeCorrelation(String columnA, String columnB) throws DDFException;


    /**
     * @see IAggregationHandler#aggregate(String)
     */
    IAggregationHandler.AggregationResult aggregate(String query) throws DDFException;

    /**
     * @see IAggregationHandler#aggregate(List)
     */
    @Deprecated
    IAggregationHandler.AggregationResult aggregate(List<IAggregationHandler.AggregateField> fields) throws DDFException;

    /**
     * @see IAggregationHandler#aggregate(String, IAggregationHandler.AggregateFunction)
     */
    double aggregate(String column, IAggregationHandler.AggregateFunction function) throws DDFException;


    /**
     * @see IAggregationHandler#groupBy(List, List)
     */
    IDDF groupBy(List<String> columns, List<String> functions) throws DDFException;

    /**
     * @see IAggregationHandler#xtabs(List)
     */
    @Deprecated
    IAggregationHandler.AggregationResult xtabs(List<IAggregationHandler.AggregateField> fields) throws DDFException;

    /**
     * @see IAggregationHandler#xtabs(String)
     */
    IAggregationHandler.AggregationResult xtabs(String fields) throws DDFException;

    //</editor-fold>
    //<editor-fold desc="BinningHandler API>

    /**
     * @see IBinningHandler#binning(String, String, int, double[], boolean, boolean)
     */
    @Deprecated
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                        boolean right) throws DDFException;

    /**
     * @see IBinningHandler#binningCustom(String, double[], boolean, boolean)
     */
    public IDDF binningCustom(String column,double[] breaks, boolean includeLowest,boolean right) throws DDFException;

    /**
     * @see IBinningHandler#binningEq(String, int, boolean, boolean)
     */
    public IDDF binningEq(String column,int numBins, boolean includeLowest,boolean right) throws DDFException;

    /**
     * @see IBinningHandler#binningEqFreq(String, int, boolean, boolean)
     */
    public IDDF binningEqFreq(String column,int numBins, boolean includeLowest,boolean right) throws DDFException;

    //</editor-fold>
    //<editor-fold desc="TransformHandler API>

    /**
     * @see ITransformHandler#transformScaleMinMax()
     */
    IDDF transformScaleMinMax() throws DDFException;

    /**
     * @see ITransformHandler#transformScaleStandard()
     */
    IDDF transformScaleStandard() throws DDFException;

    /**
     * @see ITransformHandler#transformNativeRserve(String)
     */
    IDDF transformNativeRserve(String transformExpression) throws DDFException;

    /**
     * @see ITransformHandler#transformNativeRserve(String[])
     */
    IDDF transformNativeRserve(String[] transformExpression) throws DDFException;

    /**
     * @see ITransformHandler#transformPython(String[], String[], String[], String[][])
     */
    IDDF transformPython(String[] transformFunctions, String[] functionNames,
                         String[] destColumns, String[][] sourceColumns) throws DDFException;

    /**
     * @see ITransformHandler#transformMapReduceNative(String, String, boolean)
     */
    IDDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) throws DDFException;

    /**
     * @see ITransformHandler#transformUDF(List, List)
     */
    public IDDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException;

    /**
     * @see ITransformHandler#flattenDDF(String[])
     */
    IDDF flattenDDF(String[] columns) throws DDFException;

    /**
     * @see ITransformHandler#flattenDDF()
     */
    IDDF flattenDDF() throws DDFException;

    //</editor-fold>



}
 
