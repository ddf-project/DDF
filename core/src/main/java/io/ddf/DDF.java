/**
 * Copyright 2014 Adatao, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.ddf;


import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import io.basic.ddf.BasicDDFManager;
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;
import io.ddf.analytics.IHandleAggregation;
import io.ddf.analytics.IHandleBinning;
import io.ddf.analytics.ISupportStatistics;
import io.ddf.analytics.Summary;
import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.content.*;
import io.ddf.content.IHandlePersistence.IPersistible;
import io.ddf.content.Schema.Column;
import io.ddf.etl.*;
import io.ddf.etl.IHandleMissingData.Axis;
import io.ddf.etl.IHandleMissingData.NAChecking;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.facades.*;
import io.ddf.misc.*;
import io.ddf.ml.ISupportML;
import io.ddf.ml.ISupportMLMetrics;
import io.ddf.types.AGloballyAddressable;
import io.ddf.types.AggregateTypes.AggregateField;
import io.ddf.types.AggregateTypes.AggregationResult;
import io.ddf.types.IGloballyAddressable;
import io.ddf.util.ISupportPhantomReference;
import io.ddf.util.PhantomReference;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.describe.DescribeTable;
import net.sf.jsqlparser.statement.show.ShowColumns;
import net.sf.jsqlparser.statement.show.ShowTables;
import java.io.StringReader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * A Distributed DataFrame (DDF) has a number of key properties (metadata, representations, etc.) and capabilities
 * (self-compute basic statistics, aggregations, etc.).
 * </p>
 * <p>
 * This class was designed using the Bridge Pattern to provide clean separation between the abstract concepts and the
 * implementation so that the API can support multiple big data platforms under the same set of abstract concepts.
 * </p>
 */
public abstract class DDF extends ALoggable //
    implements IGloballyAddressable, IPersistible, ISupportPhantomReference, ISerializable {

  private static final long serialVersionUID = -2198317495102277825L;

  private Date mCreatedTime;


  /**
   *
   * @param data
   *          The DDF data
   * @param namespace
   *          The namespace to place this DDF in. If null, it will be picked up from the DDFManager's current namespace.
   * @param name
   *          The name for this DDF. If null, it will come from the given schema. If that's null, a UUID-based name will
   *          be generated.
   * @param schema
   *          The {@link Schema} of the new DDF
   * @throws DDFException
   */
  public DDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {

    this.initialize(manager, data, typeSpecs, namespace, name, schema);
  }

  protected DDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
    this(manager != null ? manager : defaultManagerIfNull, null, null, null, null, null);
  }

  /**
   * This is intended primarily to provide a dummy DDF only. This signature must be provided by each implementor.
   *
   * @param manager
   * @throws DDFException
   */
  protected DDF(DDFManager manager) throws DDFException {
    this(manager, sDummyManager);
  }


  /**
   * cache for data computed from the DDF, e.g., ML models, DDF summary
   */
  protected HashMap<String, Object> cachedObjects = new HashMap<String, Object>();


  /**
   * Save a given object in memory for later (quick server-side) retrieval
   *
   * @param obj
   * @return
   */
  public String putObject(Object obj) {
    String objectId = UUID.randomUUID().toString();
    cachedObjects.put(objectId, obj);
    return objectId;
  }

  public String putObject(String objectId, Object obj) {
    cachedObjects.put(objectId, obj);
    return objectId;
  }

  /**
   * Retrieve an earlier saved object given its ID
   *
   * @param objectId
   * @return
   */
  public Object getObject(String objectId) {
    return cachedObjects.get(objectId);
  }

  /**
   * Available for run-time instantiation only.
   *
   * @throws DDFException
   */
  protected DDF() throws DDFException {
    this(sDummyManager);
  }

  /**
   * Initialization to be done after constructor assignments, such as setting of the all-important DDFManager.
   */
  protected void initialize(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name,
      Schema schema) throws DDFException {
    this.setManager(manager); // this must be done first in case later stuff needs a manager

    this.getRepresentationHandler().set(data, typeSpecs);

    this.getSchemaHandler().setSchema(schema);
    if(schema!= null && schema.getTableName() == null) {
      String tableName = this.getSchemaHandler().newTableName();
      schema.setTableName(tableName);
    }

    if (Strings.isNullOrEmpty(namespace)) namespace = this.getManager().getNamespace();
    this.setNamespace(namespace);

    manager.setDDFUUID(this, UUID.randomUUID());

    if(!Strings.isNullOrEmpty(name)) manager.setDDFName(this, name);

    // Facades
    this.ML = new MLFacade(this, this.getMLSupporter());
    this.VIEWS = new ViewsFacade(this, this.getViewHandler());
    this.Transform = new TransformFacade(this, this.getTransformationHandler());
    this.R = new RFacade(this, this.getAggregationHandler());
    this.mCreatedTime = new Date();
  }


  // ////// Instance Fields & Methods ////////



  // //// IGloballyAddressable //////

  @Expose private String mNamespace;

  @Expose private String mName;

  @Expose private UUID uuid;
  /**
   * @return the namespace this DDF belongs in
   * @throws DDFException
   */
  @Override
  public String getNamespace() {
    if (mNamespace == null) {
      try {
        mNamespace = this.getManager().getNamespace();
      } catch (Exception e) {
        mLog.warn("Cannot retrieve namespace for DDF " + this.getName(), e);
      }
    }
    return mNamespace;
  }


  /**
   * @param namespace
   *          the namespace to place this DDF in
   */
  @Override
  public void setNamespace(String namespace) {
    this.mNamespace = namespace;
  }

  /**
   *
   * @return the name of this DDF
   */
  @Override
  public String getName() {
    return mName;
  }

  /**
   * @param name
   *          the DDF name to set
   */
  protected void setName(String name) throws DDFException {
    if(name != null) validateName(name);

    this.mName = name;
  }

  //Ensure name is unique
  //Also only allow alphanumberic and dash "-" and underscore "_"
  private void validateName(String name) throws DDFException {
    Boolean isNameExisted;
    try {
      this.getManager().getDDFByName(name);
      isNameExisted = true;
    } catch (DDFException e) {
      isNameExisted = false;
    }
    if(isNameExisted) {
      throw new DDFException(String.format("DDF with name %s already exists", name));
    }

    Pattern p = Pattern.compile("^[a-zA-Z0-9_-]*$");
    Matcher m = p.matcher(name);
    if(!m.find()) {
      throw new DDFException(String.format("Invalid name %s, only allow alphanumeric (uppercase and lowercase a-z, numbers 0-9) " +
              "and dash (\"-\") and underscore (\"_\")", name));
    }
  }

  @Override
  public String getGlobalObjectType() {
    return "ddf";
  }

  public UUID getUUID() {return uuid;}

  protected void setUUID(UUID uuid) {this.uuid = uuid;}

  /**
   * We provide a "dummy" DDF Manager in case our manager is not set for some reason. (This may lead to nothing good).
   */
  private static final DDFManager sDummyManager = new BasicDDFManager();

  private DDFManager mManager;


  /**
   * Returns the previously set manager, or sets it to a dummy manager if null. We provide a "dummy" DDF Manager in case
   * our manager is not set for some reason. (This may lead to nothing good).
   *
   * @return
   */
  public DDFManager getManager() {
    if (mManager == null) mManager = sDummyManager;
    return mManager;
  }

  public void setManager(DDFManager DDFManager) {
    this.mManager = DDFManager;
  }

  /**
   *
   * @return The engine name we are built on, e.g., "spark" or "java_collections"
   */
  public String getEngine() {
    return this.getManager().getEngine();
  }

  // ////// MetaData that deserves to be right here at the top level ////////

  public Schema getSchema() {
    return this.getSchemaHandler().getSchema();
  }

  public Column getColumn(String column) {
    return this.getSchema().getColumn(column);
  }

  public String getTableName() {
    return this.getSchema().getTableName();
  }

  public List<String> getColumnNames() {
    return this.getSchema().getColumnNames();
  }

  public void setColumnNames(List<String> columnNames) {this.getSchema().setColumnNames(columnNames);}


  public long getNumRows() throws DDFException {
    return this.getMetaDataHandler().getNumRows();
  }

  public int getNumColumns() {
    return this.getSchemaHandler().getNumColumns();
  }

  public Date getCreatedTime() {
    return this.mCreatedTime;
  }

  // ///// Execute a sqlcmd
  public SqlResult sql(String sqlCommand, String errorMessage) throws DDFException {
    try {
      sqlCommand = sqlCommand.replace("@this", this.getTableName());
      // TODO: what is format?
      return this.getManager().sql(String.format(sqlCommand, this.getTableName()));
    } catch (Exception e) {
      throw new DDFException(String.format(errorMessage, this.getTableName()), e);
    }
  }

  public SqlTypedResult sqlTyped(String sqlCommand, String errorMessage) throws  DDFException {
    try {
      sqlCommand = sqlCommand.replace("@this", this.getTableName());
      return this.getManager().sqlTyped(String.format(sqlCommand, this.getTableName()));
    } catch (Exception e) {
      throw new DDFException(String.format(errorMessage, this.getTableName()), e);
    }
  }

  public DDF sql2ddf(String sqlCommand) throws DDFException {
    try {
      sqlCommand = sqlCommand.replace("@this", this.getTableName());
      return this.getManager().sql2ddf(sqlCommand);
    } catch (Exception e) {
      throw new DDFException(String.format("Error executing queries for ddf %s", this.getTableName()), e);
    }
  }

  // ///// Aggregate operations

  public RFacade R;


  public DDF getFlattenedDDF(String[] columns) throws DDFException {
    return this.getTransformationHandler().flattenDDF(columns);
  }
  public DDF getFlattenedDDF() throws DDFException {
    return this.getTransformationHandler().flattenDDF();
  }

  public DDF transform(String transformExpression) throws DDFException {
    return Transform.transformUDF(transformExpression);
  }

  /**
   *
   * @param columnA
   * @param columnB
   * @return correlation value of columnA and columnB
   * @throws DDFException
   */
  public double correlation(String columnA, String columnB) throws DDFException {
    return this.getAggregationHandler().computeCorrelation(columnA, columnB);
  }

  /**
   * Compute aggregation which is equivalent to SQL aggregation statement like
   * "SELECT a, b, sum(c), max(d) FROM e GROUP BY a, b"
   *
   * @param fields
   *          a string includes aggregated fields and functions, e.g "a, b, sum(c), max(d)"
   * @return
   * @throws DDFException
   */
  public AggregationResult aggregate(String fields) throws DDFException {
    return this.getAggregationHandler().aggregate(AggregateField.fromSqlFieldSpecs(fields));
  }

  public AggregationResult xtabs(String fields) throws DDFException {
    return this.getAggregationHandler().xtabs(AggregateField.fromSqlFieldSpecs(fields));
  }

  public DDF join(DDF anotherDDF, JoinType joinType, List<String> byColumns, List<String> byLeftColumns,
      List<String> byRightColumns) throws DDFException {
    return this.getJoinsHandler().join(anotherDDF, joinType, byColumns, byLeftColumns, byRightColumns);
  }

  public DDF groupBy(List<String> groupedColumns, List<String> aggregateFunctions) throws DDFException {
    return this.getAggregationHandler().groupBy(groupedColumns, aggregateFunctions);
  }

  public DDF groupBy(List<String> groupedColumns) {
    return this.getAggregationHandler().groupBy(groupedColumns);
  }

  public DDF agg(List<String> aggregateFunctions) throws DDFException {
    return this.getAggregationHandler().agg(aggregateFunctions);
  }

  // ///// binning
  public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
      boolean right) throws DDFException {
    return this.getBinningHandler().binning(column, binningType, numBins, breaks, includeLowest, right);
  }


  // ////// Function-Group Handlers ////////

  private ISupportStatistics mStatisticsSupporter;
  private IHandleIndexing mIndexingHandler;
  private IHandleJoins mJoinsHandler;
  private IHandleMetaData mMetaDataHandler;
  private IHandleMiscellany mMiscellanyHandler;
  private IHandleMissingData mMissingDataHandler;
  private IHandleMutability mMutabilityHandler;
  private IHandleSql mSqlHandler;
  private IHandlePersistence mPersistenceHandler;
  private IHandleRepresentations mRepresentationHandler;
  private IHandleReshaping mReshapingHandler;
  private IHandleSchema mSchemaHandler;
  private IHandleStreamingData mStreamingDataHandler;
  private IHandleTimeSeries mTimeSeriesHandler;
  private IHandleViews mViewHandler;
  private ISupportML mMLSupporter;
  private ISupportMLMetrics mMLMetricsSupporter;
  private IHandleAggregation mAggregationHandler;
  private IHandleBinning mBinningHandler;
  private IHandleTransformations mTransformationHandler;



  public ISupportStatistics getStatisticsSupporter() {
    if (mStatisticsSupporter == null) mStatisticsSupporter = this.createStatisticsSupporter();
    if (mStatisticsSupporter == null) throw new UnsupportedOperationException();
    else return mStatisticsSupporter;
  }

  public DDF setStatisticsSupporter(ISupportStatistics aStatisticsSupporter) {
    this.mStatisticsSupporter = aStatisticsSupporter;
    return this;
  }

  protected ISupportStatistics createStatisticsSupporter() {
    return newHandler(ISupportStatistics.class);
  }


  public IHandleIndexing getIndexingHandler() {
    if (mIndexingHandler == null) mIndexingHandler = this.createIndexingHandler();
    if (mIndexingHandler == null) throw new UnsupportedOperationException();
    else return mIndexingHandler;
  }

  public DDF setIndexingHandler(IHandleIndexing anIndexingHandler) {
    this.mIndexingHandler = anIndexingHandler;
    return this;
  }

  protected IHandleIndexing createIndexingHandler() {
    return newHandler(IHandleIndexing.class);
  }


  public IHandleJoins getJoinsHandler() {
    if (mJoinsHandler == null) mJoinsHandler = this.createJoinsHandler();
    if (mJoinsHandler == null) throw new UnsupportedOperationException();
    else return mJoinsHandler;
  }

  public DDF setJoinsHandler(IHandleJoins aJoinsHandler) {
    this.mJoinsHandler = aJoinsHandler;
    return this;
  }

  protected IHandleJoins createJoinsHandler() {
    return newHandler(IHandleJoins.class);
  }


  public IHandleMetaData getMetaDataHandler() {
    if (mMetaDataHandler == null) mMetaDataHandler = this.createMetaDataHandler();
    if (mMetaDataHandler == null) throw new UnsupportedOperationException();
    else return mMetaDataHandler;
  }

  public DDF setMetaDataHandler(IHandleMetaData aMetaDataHandler) {
    this.mMetaDataHandler = aMetaDataHandler;
    return this;
  }

  protected IHandleMetaData createMetaDataHandler() {
    return newHandler(IHandleMetaData.class);
  }


  public IHandleMiscellany getMiscellanyHandler() {
    if (mMiscellanyHandler == null) mMiscellanyHandler = this.createMiscellanyHandler();
    if (mMiscellanyHandler == null) throw new UnsupportedOperationException();
    else return mMiscellanyHandler;
  }

  public DDF setMiscellanyHandler(IHandleMiscellany aMiscellanyHandler) {
    this.mMiscellanyHandler = aMiscellanyHandler;
    return this;
  }

  protected IHandleMiscellany createMiscellanyHandler() {
    return newHandler(IHandleMiscellany.class);
  }


  public IHandleMissingData getMissingDataHandler() {
    if (mMissingDataHandler == null) mMissingDataHandler = this.createMissingDataHandler();
    if (mMissingDataHandler == null) throw new UnsupportedOperationException();
    else return mMissingDataHandler;
  }

  public DDF setMissingDataHandler(IHandleMissingData aMissingDataHandler) {
    this.mMissingDataHandler = aMissingDataHandler;
    return this;
  }

  protected IHandleMissingData createMissingDataHandler() {
    return newHandler(IHandleMissingData.class);
  }

  public IHandleAggregation getAggregationHandler() {
    if (mAggregationHandler == null) mAggregationHandler = this.createAggregationHandler();
    if (mAggregationHandler == null) throw new UnsupportedOperationException();
    else return mAggregationHandler;
  }

  public DDF setAggregationHandler(IHandleAggregation aAggregationHandler) {
    this.mAggregationHandler = aAggregationHandler;
    return this;
  }

  protected IHandleAggregation createAggregationHandler() {
    return newHandler(IHandleAggregation.class);
  }

  public IHandleBinning getBinningHandler() {
    if (mBinningHandler == null) mBinningHandler = this.createBinningHandler();
    if (mBinningHandler == null) throw new UnsupportedOperationException();
    else return mBinningHandler;
  }

  public DDF setBinningHandler(IHandleBinning aBinningHandler) {
    this.mBinningHandler = aBinningHandler;
    return this;
  }

  protected IHandleBinning createBinningHandler() {
    return newHandler(IHandleBinning.class);
  }

  public IHandleTransformations getTransformationHandler() {
    if (mTransformationHandler == null) mTransformationHandler = this.createTransformationHandler();
    if (mTransformationHandler == null) throw new UnsupportedOperationException();
    else return mTransformationHandler;
  }

  public DDF setTransformationHandler(IHandleTransformations aTransformationHandler) {
    this.mTransformationHandler = aTransformationHandler;
    return this;
  }

  protected IHandleTransformations createTransformationHandler() {
    return newHandler(IHandleTransformations.class);
  }

  public IHandleMutability getMutabilityHandler() {
    if (mMutabilityHandler == null) mMutabilityHandler = this.createMutabilityHandler();
    if (mMutabilityHandler == null) throw new UnsupportedOperationException();
    else return mMutabilityHandler;
  }

  public DDF setMutabilityHandler(IHandleMutability aMutabilityHandler) {
    this.mMutabilityHandler = aMutabilityHandler;
    return this;
  }

  protected IHandleMutability createMutabilityHandler() {
    return newHandler(IHandleMutability.class);
  }


  public IHandleSql getSqlHandler() {
    if (mSqlHandler == null) mSqlHandler = this.createSqlHandler();
    if (mSqlHandler == null) throw new UnsupportedOperationException();
    else return mSqlHandler;
  }

  public DDF setSqlHandler(IHandleSql aSqlHandler) {
    this.mSqlHandler = aSqlHandler;
    return this;
  }

  protected IHandleSql createSqlHandler() {
    return newHandler(IHandleSql.class);
  }


  public IHandlePersistence getPersistenceHandler() {
    if (mPersistenceHandler == null) mPersistenceHandler = this.createPersistenceHandler();
    if (mPersistenceHandler == null) throw new UnsupportedOperationException();
    else return mPersistenceHandler;
  }

  public DDF setPersistenceHandler(IHandlePersistence aPersistenceHandler) {
    this.mPersistenceHandler = aPersistenceHandler;
    return this;
  }

  protected IHandlePersistence createPersistenceHandler() {
    return newHandler(IHandlePersistence.class);
  }


  public IHandleRepresentations getRepresentationHandler() {
    if (mRepresentationHandler == null) mRepresentationHandler = this.createRepresentationHandler();
    if (mRepresentationHandler == null) throw new UnsupportedOperationException();
    else return mRepresentationHandler;
  }

  public DDF setRepresentationHandler(IHandleRepresentations aRepresentationHandler) {
    this.mRepresentationHandler = aRepresentationHandler;
    return this;
  }

  protected IHandleRepresentations createRepresentationHandler() {
    return newHandler(IHandleRepresentations.class);
  }


  public IHandleReshaping getReshapingHandler() {
    if (mReshapingHandler == null) mReshapingHandler = this.createReshapingHandler();
    if (mReshapingHandler == null) throw new UnsupportedOperationException();
    else return mReshapingHandler;
  }

  public DDF setReshapingHandler(IHandleReshaping aReshapingHandler) {
    this.mReshapingHandler = aReshapingHandler;
    return this;
  }

  protected IHandleReshaping createReshapingHandler() {
    return newHandler(IHandleReshaping.class);
  }


  public IHandleSchema getSchemaHandler() {
    if (mSchemaHandler == null) mSchemaHandler = this.createSchemaHandler();
    if (mSchemaHandler == null) throw new UnsupportedOperationException();
    else return mSchemaHandler;
  }

  public DDF setSchemaHandler(IHandleSchema aSchemaHandler) {
    this.mSchemaHandler = aSchemaHandler;
    return this;
  }

  protected IHandleSchema createSchemaHandler() {
    return newHandler(IHandleSchema.class);
  }


  public IHandleStreamingData getStreamingDataHandler() {
    if (mStreamingDataHandler == null) mStreamingDataHandler = this.createStreamingDataHandler();
    if (mStreamingDataHandler == null) throw new UnsupportedOperationException();
    else return mStreamingDataHandler;
  }

  public DDF setStreamingDataHandler(IHandleStreamingData aStreamingDataHandler) {
    this.mStreamingDataHandler = aStreamingDataHandler;
    return this;
  }

  protected IHandleStreamingData createStreamingDataHandler() {
    return newHandler(IHandleStreamingData.class);
  }


  public IHandleTimeSeries getTimeSeriesHandler() {
    if (mTimeSeriesHandler == null) mTimeSeriesHandler = this.createTimeSeriesHandler();
    if (mTimeSeriesHandler == null) throw new UnsupportedOperationException();
    else return mTimeSeriesHandler;
  }

  public DDF setTimeSeriesHandler(IHandleTimeSeries aTimeSeriesHandler) {
    this.mTimeSeriesHandler = aTimeSeriesHandler;
    return this;
  }

  protected IHandleTimeSeries createTimeSeriesHandler() {
    return newHandler(IHandleTimeSeries.class);
  }


  public IHandleViews getViewHandler() {
    if (mViewHandler == null) mViewHandler = this.createViewHandler();
    if (mViewHandler == null) throw new UnsupportedOperationException();
    else return mViewHandler;
  }

  public DDF setViewHandler(IHandleViews aViewHandler) {
    this.mViewHandler = aViewHandler;
    return this;
  }

  protected IHandleViews createViewHandler() {
    return newHandler(IHandleViews.class);
  }

  public ISupportML getMLSupporter() {
    if (mMLSupporter == null) mMLSupporter = this.createMLSupporter();
    if (mMLSupporter == null) throw new UnsupportedOperationException();
    else return mMLSupporter;
  }

  public DDF setMLSupporter(ISupportML aMLSupporter) {
    this.mMLSupporter = aMLSupporter;
    return this;
  }

  protected ISupportML createMLSupporter() {
    return newHandler(ISupportML.class);
  }

  // Metrics supporter
  protected ISupportMLMetrics createMLMetricsSupporter() {
    return newHandler(ISupportMLMetrics.class);
  }

  public ISupportMLMetrics getMLMetricsSupporter() {
    if (mMLMetricsSupporter == null) mMLMetricsSupporter = this.createMLMetricsSupporter();
    if (mMLMetricsSupporter == null) throw new UnsupportedOperationException();
    else return mMLMetricsSupporter;
  }

  public DDF setMLMetricsSupporter(ISupportMLMetrics aMLMetricsSupporter) {
    this.mMLMetricsSupporter = aMLMetricsSupporter;
    return this;
  }



  /**
   * Instantiate a new {@link ADDFFunctionalGroupHandler} given its class name
   *
   * @param className
   * @return
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   * @throws SecurityException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  @SuppressWarnings("unchecked")
  protected <I> I newHandler(Class<I> theInterface) {
    if (theInterface == null) return null;

    String className = null;

    try {
      className = Config.getValueWithGlobalDefault(this.getEngine(), theInterface.getSimpleName());
      mLog.info(">>> className = " + className);
      if (Strings.isNullOrEmpty(className)) {
        mLog.error(String.format("Cannot determine classname for %s from configuration source [%s] %s",
            theInterface.getSimpleName(), Config.getConfigHandler().getSource(), this.getEngine()));
        return null;
      }

      Class<?> clazz = Class.forName(className);

      if (Modifier.isAbstract(clazz.getModifiers())) {
        throw new InstantiationError(String.format("Class %s is abstract and cannot be instantiated", className));
      }

      Constructor<ADDFFunctionalGroupHandler> cons = (Constructor<ADDFFunctionalGroupHandler>) clazz
          .getDeclaredConstructor(new Class<?>[] { DDF.class });

      if (cons != null) cons.setAccessible(true);

      return cons != null ? (I) cons.newInstance(this) : null;

    } catch (ClassNotFoundException cnfe) {
      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), cnfe);
    } catch (NoSuchMethodException nsme) {
      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), nsme);
    } catch (IllegalAccessException iae) {
      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), iae);
    } catch (InstantiationException ie) {
      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), ie);
    } catch (InvocationTargetException ite) {
      mLog.error(String.format("Cannot instantiate handler for [%s] %s/%s", this.getEngine(),
          theInterface.getSimpleName(), className), ite);
    }
    return null;
  }


  @Override
  public String getUri() {
    return AGloballyAddressable.getUri(this);
  }


  @Override
  public String toString() {
    return this.getUri();
  }


  public void setMutable(boolean isMutable) {
    this.getMutabilityHandler().setMutable(isMutable);
  }

  public boolean isMutable() {
    return this.getMutabilityHandler().isMutable();
  }

  /**
   * This will be called via the {@link ISupportPhantomReference} interface if this object was registered under
   * {@link PhantomReference}.
   */
  @Override
  public void cleanup() {
    // @formatter:off
    this
      .setMLSupporter(null)
      .setStatisticsSupporter(null)
      .setIndexingHandler(null)
      .setJoinsHandler(null)
      .setMetaDataHandler(null)
      .setMiscellanyHandler(null)
      .setMissingDataHandler(null)
      .setMutabilityHandler(null)
      .setSqlHandler(null)
      .setPersistenceHandler(null)
      .setRepresentationHandler(null)
      .setReshapingHandler(null)
      .setSchemaHandler(null)
      .setStreamingDataHandler(null)
      .setTimeSeriesHandler(null)
      ;
    // @formatter:on
  }



  // //// IHandleSchema //////

  /**
   * @param columnName
   * @return
   */
  public int getColumnIndex(String columnName) {
    return this.getSchema().getColumnIndex(columnName);
  }

  public String getColumnName(int columnIndex) {
    return this.getSchema().getColumnName(columnIndex);
  }

  public Factor<?> setAsFactor(int columnIndex) {
    return this.getSchemaHandler().setAsFactor(columnIndex);
  }

  public Factor<?> setAsFactor(String columnName) {
    return this.getSchemaHandler().setAsFactor(columnName);
  }

  public void unsetAsFactor(int columnIndex) {
    this.getSchemaHandler().unsetAsFactor(columnIndex);
  }

  public void unsetAsFactor(String columnName) {
    this.getSchemaHandler().unsetAsFactor(columnName);
  }


  // //// IHandleViews //////

  public ViewsFacade VIEWS;


  // public <T> Iterator<T> getRowIterator(Class<T> dataType) {
  // return this.getViewHandler().getRowIterator(dataType);
  // }
  //
  // public Iterator<?> getRowIterator() {
  // return this.getViewHandler().getRowIterator();
  // }
  //
  // public <D, C> Iterator<C> getElementIterator(Class<D> dataType, Class<C> columnType, String columnName) {
  // return this.getViewHandler().getElementIterator(dataType, columnType, columnName);
  // }
  //
  // public Iterator<?> getElementIterator(String columnName) {
  // return this.getViewHandler().getElementIterator(columnName);
  // }



  // //// ISupportStatistics //////

  // Calculate summary statistics of the DDF
  public Summary[] getSummary() throws DDFException {
    return this.getStatisticsSupporter().getSummary();
  }

  public FiveNumSummary[] getFiveNumSummary() throws DDFException {
    return this.getStatisticsSupporter().getFiveNumSummary(this.getColumnNames());
  }


  // IHandleTransformations

  // Transformations

  public TransformFacade Transform;


  public DDF dropNA() throws DDFException {
    return dropNA(Axis.ROW);
  }

  public DDF dropNA(Axis pattern) throws DDFException {
    return this.getMissingDataHandler().dropNA(pattern, NAChecking.ANY, 0, null);
  }

  public DDF fillNA(String value) throws DDFException {
    return this.getMissingDataHandler().fillNA(value, null, 0, null, null, null);
  }

  public DDF updateInplace(DDF result) throws DDFException {
    return this.getMutabilityHandler().updateInplace(result);
  }

  public Double[] getVectorQuantiles(String columnName, Double[] percentiles) throws DDFException {
    return this.getStatisticsSupporter().getVectorQuantiles(columnName, percentiles);
  }

  public Double[] getVectorQuantiles(Double[] percentiles) throws DDFException {
    if (getSchema().getNumColumns() != 1) {
      throw new DDFException("This method only applies to one columned DDF.");
    }
    return this.getStatisticsSupporter().getVectorQuantiles(getSchema().getColumn(0).getName(), percentiles);
  }

  public Double[] getVectorVariance(String columnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorVariance(columnName);
  }

  public Double getVectorMean(String columnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorMean(columnName);
  }

  // for backward compatibility
  public List<HistogramBin> getVectorHistogram_Hive(String columnName, int numBins) throws DDFException {
    return getVectorApproxHistogram(columnName, numBins);
  }
  public List<HistogramBin> getVectorApproxHistogram(String columnName, int numBins) throws DDFException {
    // TODO need to check columnName
    return this.getBinningHandler().getVectorApproxHistogram(columnName, numBins);

  }

  public List<HistogramBin> getVectorHistogram(String columnName, int numBins) throws DDFException {
    // TODO need to check columnName
    return this.getBinningHandler().getVectorHistogram(columnName, numBins);
  }

  public Double getVectorCor(String xColumnName, String yColumnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorCor(xColumnName, yColumnName);
  }

  public Double getVectorCovariance(String xColumnName, String yColumnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorCovariance(xColumnName, yColumnName);
  }

  public Double getVectorMin(String columnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorMin(columnName);
  }

  public Double getVectorMax(String columnName) throws DDFException {
    // TODO need to check columnName
    return this.getStatisticsSupporter().getVectorMax(columnName);
  }


  // //// ISupportML //////
  public MLFacade ML;



  // //// IHandlePersistence //////

  public PersistenceUri persist() throws DDFException {
    return this.persist(true);
  }

  @Override
  public PersistenceUri persist(boolean doOverwrite) throws DDFException {
    return this.getPersistenceHandler().persist(doOverwrite);
  }

  @Override
  public void unpersist() throws DDFException {
    this.getManager().unpersist(this.getNamespace(), this.getName());
  }


  /**
   * The base implementation checks if the schema is null, and if so, generate a generic one. This is useful/necessary
   * before persistence, to avoid the situtation of null schemas being persisted.
   */
  @Override
  public void beforePersisting() throws DDFException {
    if (this.getSchema() == null) this.getSchemaHandler().setSchema(this.getSchemaHandler().generateSchema());
  }

  @Override
  public void afterPersisting() {
  }

  @Override
  public void beforeUnpersisting() {
  }

  @Override
  public void afterUnpersisting() {
  }

  //
  // //// ISerializable //////

  @Override
  public void beforeSerialization() throws DDFException {
  }

  @Override
  public void afterSerialization() throws DDFException {
  }

  @Override
  public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData)
      throws DDFException {
    return deserializedObject;
  }

}
