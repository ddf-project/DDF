/**
 * Copyright 2014 Adatao, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ddf;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.content.IHandlePersistence.IPersistible;
import io.ddf.content.IHandleRepresentations;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.content.SqlTypedResult;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.DataSourceManager;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.etl.IHandleSqlLike;
import io.ddf.exception.DDFException;
import io.ddf.misc.ALoggable;
import io.ddf.misc.Config;
import io.ddf.misc.Config.ConfigConstant;
import io.ddf.misc.ObjectRegistry;
import io.ddf.ml.IModel;
import io.ddf.ml.ISupportML;
import io.ddf.util.ISupportPhantomReference;
import io.ddf.util.PhantomReference;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Abstract base class for a {@link DDF} implementor, which provides the support methods necessary to implement various
 * DDF interfaces, such as {@link IHandleRepresentations} and {@link ISupportML}.
 * </p>
 * <p>
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to provide alternative
 * (even snap-in replacements), support/implementation for DDF. The class diagram is as follows:
 * </p>
 * <p/>
 * <pre>
 * ------------------   -------------------------
 * |     DDFManager |-->|         DDF           |
 * ------------------   -------------------------
 *                         ^          ^
 *                         |   ...    |        -------------------
 *                         |          |------->| IHandleMetadata |
 *                         |                   -------------------
 *                         |
 *                         |        ----------------------------------
 *                         |------->| IHandleRepresentations |
 *                                  ----------------------------------
 * </pre>
 * <p>
 * An implementor need not provide all or even most of these interfaces. Each interface handler can be get/set
 * separately, as long as they cooperate properly on things like the underlying representation. This makes it easy to
 * roll out additional interfaces and their implementations over time.
 * </p>
 * DDFManager implements {@link IHandleSqlLike} because we want to expose those methods as directly to the API user as
 * possible, in an engine-dependent manner.
 */
public abstract class DDFManager extends ALoggable implements IDDFManager, IHandleSqlLike, ISupportPhantomReference {

  public enum EngineType {
    SPARK,
    JDBC,
    SFDC,
    POSTGRES,
    AWS,
    REDSHIFT,
    BASIC
    ;

    public static EngineType fromString(String str) throws DDFException {
      if (str.equalsIgnoreCase("spark")) {
        return SPARK;
      } else if (str.equalsIgnoreCase("jdbc")) {
        return JDBC;
      } else if (str.equalsIgnoreCase("sfdc")) {
        return SFDC;
      } else if (str.equalsIgnoreCase("postgres")) {
        return POSTGRES;
      } else if(str.equalsIgnoreCase("aws")) {
        return AWS;
      } else if(str.equalsIgnoreCase("redshift")) {
        return REDSHIFT;
      } else if(str.equalsIgnoreCase("basic")) {
        return BASIC;
      } else {
        throw new DDFException("Engine type should be either spark, jdbc, postgres, aws, redshift, basic");
      }
    }
  }


  // The engine name, should be unique.
  private UUID uuid = UUID.randomUUID();
  private EngineType engineType;
  // DataSourceDescriptor.
  private DataSourceDescriptor mDataSourceDescriptor;
  // DDFCoordinator.
  private DDFCoordinator mDDFCoordinator;


  public UUID getUUID() {
    return uuid;
  }

  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  public EngineType getEngineType() {
    return engineType;
  }

  public void setEngineType(EngineType engineType) {
    this.engineType = engineType;
  }

  public DDFCoordinator getDDFCoordinator() {
    return mDDFCoordinator;
  }

  public void setDDFCoordinator(DDFCoordinator ddfCoordinator) {
    this.mDDFCoordinator = ddfCoordinator;
  }

  public DataSourceDescriptor getDataSourceDescriptor() {
    return mDataSourceDescriptor;
  }

  public void setDataSourceDescriptor(DataSourceDescriptor dataSourceDescriptor) {
    this.mDataSourceDescriptor = dataSourceDescriptor;
  }

  /**
   * @brief Tranfer the ddf from another engine. This should be override by
   * subclass.
   * @param fromEngine The engine where the ddf is.
   * @param ddfuuid The uuid of the ddf.
   * @return The new ddf.
   */
  public abstract DDF transfer(UUID fromEngine, UUID ddfuuid) throws
          DDFException;

  public abstract DDF transferByTable(UUID fromEngine, String tableName) throws
          DDFException;

  /**
   * List of existing DDFs, only in memory one.
   */
  protected DDFCache mDDFCache = new DDFCache();

  protected Map<String, IModel> mModels = new ConcurrentHashMap<String, IModel>();

  public void addDDF(DDF ddf) throws DDFException {
    mDDFCache.addDDF(ddf);
    if (this.mDDFCoordinator != null) {
      mDDFCoordinator.setDDFUUID2DDFManager(ddf.getUUID(), this);
    }
  }

  public void removeDDF(DDF ddf) throws DDFException {
    ddf.getRepresentationHandler().uncacheAll();
    ddf.getRepresentationHandler().reset();
    mDDFCache.removeDDF(ddf);
  }

  public DDF[] listDDFs() {
    return mDDFCache.listDDFs();
  }

  // TODO: Should we consider restore here?
  public DDF getDDF(UUID uuid) throws DDFException {
    return mDDFCache.getDDF(uuid);
  }

  // TODO: Should we consider restore here?
  public boolean hasDDF(UUID uuid) {
    return mDDFCache.hasDDF(uuid);
  }

  // TODO: Should we consider restore here?
  public DDF getDDFByName(String name) throws DDFException {
    return mDDFCache.getDDFByName(name);
  }

  public synchronized void setDDFName(DDF ddf, String name) throws DDFException {
    mDDFCache.setDDFName(ddf, name);
    mLog.info("set ddf uri : " + "ddf://" + ddf.getNamespace() + "/" + name);
  }

  public synchronized void setDDFUUID(DDF ddf, UUID uuid) throws DDFException {
    mDDFCache.setDDFUUID(ddf, uuid);
  }

  public DDF getDDFByURI(String uri) throws DDFException {
    return mDDFCache.getDDFByUri(uri);
  }

  public void addModel(IModel model) {
    mModels.put(model.getName(), model);
  }

  public IModel getModel(String modelName) {
    return mModels.get(modelName);
  }

  public DDF serialize2DDF(IModel model) throws DDFException {
    // TODO
    // DDF df = new DDF(this, model.getRawModel(), new Class[] {IModel.class} , null, model.getName(), null);
    return null;
  }

  public IModel deserialize2Model(DDF ddf) {
    // TODO
    return null;
  }

  public DDFManager() {
    this.startup();
  }

  // TODO: check the correctness here.
  public DDFManager(DataSourceDescriptor dataSourceDescriptor) {
    this.startup();
  }

  public DDFManager(DataSourceDescriptor dataSourceDescriptor, UUID engineUUID) {
    this.setUUID(engineUUID);
    this.startup();
  }

  public DDFManager(String namespace) {
    this.setNamespace(namespace);
    this.startup();
  }

  public static DDFManager get(EngineType engineType, DataSourceDescriptor dataSourceDescriptor) throws DDFException {
    if (engineType == null) {
      engineType = EngineType.fromString(ConfigConstant.ENGINE_NAME_DEFAULT.toString());
    }

    String className = Config.getValue(engineType.name(), ConfigConstant.FIELD_DDF_MANAGER);
    // if (Strings.isNullOrEmpty(className)) return null;
    if (Strings.isNullOrEmpty(className)) {
      throw new DDFException("ERROR: When initializaing ddfmanager, class " +
              className + " not found");
    }

    try {
      Class[] classType = new Class[2];
      classType[0] = DataSourceDescriptor.class;
      classType[1] = EngineType.class;

      DDFManager manager = (DDFManager) Class.forName(className).getDeclaredConstructor(classType)
          .newInstance(dataSourceDescriptor, engineType);
      UUID uuid = UUID.randomUUID();
      manager.setUUID(uuid);
      return manager;
    } catch (Exception e) {

      throw new DDFException(e);
    }
  }

  public static DDFManager get(EngineType engineType, String uri) throws DDFException {
    Preconditions.checkArgument(engineType != null, "Engine type cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(uri), "Source uri cannot be null or empty");

    String className = Config.getValue(engineType.name(), ConfigConstant.FIELD_DDF_MANAGER);
    if (Strings.isNullOrEmpty(className)) {
      throw new DDFException("Error creating DDFManager, no class configured for engine " + engineType.name());
    }

    try {
      Class<?> clazz = Class.forName(className);
      if (!DDFManager.class.isAssignableFrom(clazz)) {
        throw new DDFException("DDF manager class must extend from io.ddf.DDFManager");
      }
      return (DDFManager) clazz.getDeclaredConstructor(EngineType.class, String.class).newInstance(engineType, uri);
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }

  /**
   * Returns a new instance of {@link DDFManager} for the given engine name
   *
   * @param engineType
   * @return
   * @throws Exception
   */
  public static DDFManager get(EngineType engineType) throws DDFException {
    if (engineType == null) {
      engineType = EngineType.fromString(ConfigConstant.ENGINE_NAME_DEFAULT.toString());
    }

    String className = Config.getValue(engineType.name(), ConfigConstant.FIELD_DDF_MANAGER);
    if (Strings.isNullOrEmpty(className)) {
      throw new DDFException("ERROR: When initializaing ddfmanager, class " +
              className + " not found");
    }

    try {
      DDFManager manager = (DDFManager) Class.forName(className).newInstance();
      if (manager == null) {
        throw new DDFException("ERROR: Initializaing manager fail.");
      }
      return manager;

    } catch (Exception e) {
      // throw new DDFException("Cannot get DDFManager for engine " + engineName, e);
      throw new DDFException(
          "Cannot get DDFManager for engine " + engineType.name() + " " +
                  "classname " + className + " " + e.getMessage());

    }
  }

  private DDF mDummyDDF;

  protected DDF getDummyDDF() throws DDFException {
    if (mDummyDDF == null) mDummyDDF = this.newDDF(this);
    return mDummyDDF;
  }

  //  /**
  //   * Instantiates a new DDF of the type specified in ddf.ini as "DDF".
  //   *
  //   * @param manager
  //   * @param data
  //   * @param typeSpecs
  //   * @param namespace
  //   * @param name
  //   * @param schema
  //   * @return
  //   * @throws DDFException
  //   */
  //  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs,
  //                    String namespace, String name, Schema
  //                            schema)
  //      throws DDFException {
  //
  //    // @formatter:off
//    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
//				Class[].class, String.class, String.class, Schema
//                    .class },
//				new Object[] { manager, data, typeSpecs,
//                        namespace, name,
//						schema });
//    return ddf;
//  }

  // TODO: For back compatability.
  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs,
                    String namespace, String name, Schema schema) throws DDFException {
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
				Class[].class, String.class, String.class, Schema
                    .class },
				new Object[] { manager, data, typeSpecs,
                        namespace, name,
						schema });
    return ddf;
  }

  public DDF newDDF(Object data, Class<?>[] typeSpecs,
                    String namespace, String name, Schema schema)
      throws DDFException {

    // @formatter:off
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
						Class[].class, String.class, String.class, Schema.class },
						new Object[] { this, data, typeSpecs,
                                namespace,
                                name,
								schema });
    return ddf;
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF", using the constructor that requires only
   * {@link DDFManager} as an argument.
   *
   * @param manager the {@link DDFManager} to assign
   * @return the newly instantiated DDF
   * @throws DDFException
   */
  public DDF newDDF(DDFManager manager) throws DDFException {
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class }, new Object[] { manager });
    ddf.getPersistenceHandler().setPersistable(false);
    return ddf;
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF", passing in this DDFManager as the sole argument
   *
   * @return the newly instantiated DDF
   * @throws DDFException
   */
  public DDF newDDF() throws DDFException {
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class }, new Object[] { this });
    ddf.getPersistenceHandler().setPersistable(false);
    return ddf;
  }

  @SuppressWarnings("unchecked")
  private DDF newDDF(Class<?>[] argTypes, Object[] argValues) throws DDFException {

    String className = Config.getValueWithGlobalDefault(this.getEngine(), ConfigConstant.FIELD_DDF);
    if (Strings.isNullOrEmpty(className)) throw new DDFException(String.format(
        "Cannot determine class name for [%s] %s", this.getEngine(), "DDF"));

    try {
      Constructor<DDF> cons = (Constructor<DDF>) Class.forName(className).getDeclaredConstructor(argTypes);
      if (cons == null) throw new DDFException("Cannot get constructor for " + className);

      cons.setAccessible(true); // make sure we can use it whether it's
      // private, protected, or public

      DDF ddf = cons.newInstance(argValues);
      if (ddf == null) throw new DDFException("Cannot instantiate a new instance of " + className);
      this.addDDF(ddf);
      return ddf;

    } catch (Exception e) {
      throw new  DDFException(String.format(
          "While instantiating a new %s DDF of class %s with argTypes %s and argValues %s", this.getEngine(),
          className, Arrays.toString(argTypes), Arrays.toString(argValues)), e);
    }
  }

  // ////// ISupportPhantomReference ////////

  public void cleanup() {
    // Do nothing in the base
  }

  // ////// IDDFManager ////////

  @Override
  public void startup() {
    try {
      this.getNamespace(); // trigger the loading of the namespace

    } catch (DDFException e) {
      mLog.warn("Error while trying to getNamesapce()", e);
    }

    PhantomReference.register(this);
  }

  @Override
  public void shutdown() {
    // Do nothing in the base
  }


  private String mNamespace;


  @Override
  public String getNamespace() throws DDFException {
    if (Strings.isNullOrEmpty(mNamespace)) {
      mNamespace = Config.getValueWithGlobalDefault(this.getEngine(), ConfigConstant.FIELD_NAMESPACE);
    }

    return mNamespace;
  }

  @Override
  public void setNamespace(String namespace) {
    mNamespace = namespace;
  }


  // ////// IDDFRegistry ////////

  private static final ObjectRegistry sObjectRegistry = new ObjectRegistry();
  public final ObjectRegistry REGISTRY = sObjectRegistry;


  // ////// IHandleSql facade methods ////////
  @Override
  public DDF sql2ddf(String command) throws DDFException {
    return this.sql2ddf(command, null, null, null);
  }

  public DDF sql2ddf(String command, String dataSource) throws DDFException {
    return this.sql2ddf(command,
            new SQLDataSourceDescriptor(null, dataSource, null, null, null));
  }

  public DDF sql2ddf(String command, DataSourceDescriptor dataSource)
          throws  DDFException {
    return this.sql2ddf(command, null, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat)
          throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command,
                     Schema schema,
                     DataSourceDescriptor dataSource)
          throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat)
          throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command,
                     Schema schema,
                     DataSourceDescriptor dataSource,
                     DataFormat dataFormat) throws DDFException {
    return this.getDummyDDF().getSqlHandler().
            sql2ddfHandle(command, schema, dataSource, dataFormat);
  }


  @Override
  public SqlResult sql(String command) throws DDFException {
    return this.sql(command, (Integer) null);
  }

  public SqlResult sql(String command, String dataSource) throws DDFException {
    return this.sql(command,
            new SQLDataSourceDescriptor(null, dataSource,null, null, null));
  }

  @Override
  public SqlResult sql(String command, Integer maxRows) throws DDFException {
    return this.sql(command, maxRows, null);
  }


  @Override
  public SqlResult sql(String command,
                       Integer maxRows,
                       DataSourceDescriptor dataSource) throws DDFException {
    return this.getDummyDDF().getSqlHandler().
            sqlHandle(command, maxRows, dataSource);
  }

  public SqlResult sql(String command, DataSourceDescriptor dataSource)
          throws DDFException {
    return this.getDummyDDF().getSqlHandler().
            sqlHandle(command, null, dataSource);
  }

  @Override
  public SqlTypedResult sqlTyped(String command) throws DDFException {
    return this.sqlTyped(command, null);
  }

  @Override
  public SqlTypedResult sqlTyped(String command, Integer maxRows)
          throws DDFException {
    return this.sqlTyped(command, maxRows, null);
  }

  @Override
  public SqlTypedResult sqlTyped(String command,
                                 Integer maxRows,
                                 DataSourceDescriptor dataSource)
          throws DDFException {
    // @Note This is another possible solution, which I think is more stable.
    // return this.getDummyDDF().getSqlHandler().sqlTyped(command, maxRows, dataSource);
    return new SqlTypedResult(sql(command, maxRows, dataSource));
  }

  // //// Persistence handling //////

  public void unpersist(String namespace, String name) throws DDFException {
    this.getDummyDDF().getPersistenceHandler().unpersist(namespace, name);
  }

  public static IPersistible doLoad(String uri) throws DDFException {
    return doLoad(new PersistenceUri(uri));
  }

  public static IPersistible doLoad(PersistenceUri uri) throws DDFException {
    if (uri == null) throw new DDFException("URI cannot be null");
    if (Strings.isNullOrEmpty(uri.getEngine()))
      throw new DDFException("Engine/Protocol in URI cannot be missing");
    return DDFManager.get(EngineType.fromString(uri.getEngine())).load(uri);
  }

  public IPersistible load(String namespace, String name) throws DDFException {
    return this.getDummyDDF().getPersistenceHandler().load(namespace, name);
  }

  public IPersistible load(PersistenceUri uri) throws DDFException {
    return this.getDummyDDF().getPersistenceHandler().load(uri);
  }

  /**
   * Create DDF from a file
   * TODO: we should change the name of this function to match its functionality
   *
   * @param fileURL
   * @param fieldSeparator
   * @return
   * @throws DDFException
   */
  public abstract DDF loadTable(String fileURL, String fieldSeparator) throws DDFException;

  public DDF restoreDDF(UUID uuid) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }
  /**
   * @brief Restore the ddf given uri.
   * @param ddfURI The URI of ddf.
   * @return The ddf.
   */
  public abstract DDF getOrRestoreDDFUri(String ddfURI) throws DDFException;

  public abstract DDF getOrRestoreDDF(UUID uuid) throws DDFException;

  public DDF load(DataSourceDescriptor ds) throws DDFException {
    return (new DataSourceManager()).load(ds, this);
  }

  public abstract DDF createDDF(Map<Object, Object> options) throws DDFException;
}
