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
import io.ddf.content.*;
import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.content.IHandlePersistence.IPersistible;

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
import java.util.*;
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

  public enum  EngineType {
    ENGINE_SPARK("spark"),
    ENGINE_JDBC("jdbc"),
    ENGINE_SFDC("sfdc"),
    ENGINE_POSTGRES("postgres");

    private final String typeName;
    EngineType(String typeName) {
      this.typeName = typeName;
    }

    public String getTypeName() {
      return this.typeName;
    }

    public static EngineType fromString(String str) throws DDFException {
      if(str.equals("spark")) {
        return ENGINE_SPARK;
      } else if (str.equals("jdbc")) {
        return ENGINE_JDBC;
      } else if(str.equals("sfdc")) {
        return ENGINE_SFDC;
      } else if(str.equals("postgres")) {
        return ENGINE_POSTGRES;
      } else {
        throw new DDFException("Engine type should be either spark, jdbc or sfdc");
      }
    }
  }

  // The engine name, should be unique.
  private String engineName;
  private EngineType engineType;
  // DataSourceDescriptor.
  private DataSourceDescriptor mDataSourceDescriptor;
  // DDFCoordinator.
  private DDFCoordinator mDDFCoordinator;


  public String getEngineName() {
    return engineName;
  }

  // TODO: We should be careful about engine name. Because all the ddfs are
  // using engine name.
  public void setEngineName(String engineName) {
    this.engineName = engineName;
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

  public void setDataSourceDescriptor(DataSourceDescriptor
                                               dataSourceDescriptor) {
    this.mDataSourceDescriptor = dataSourceDescriptor;
  }

  public void log(String info) {
    this.mLog.info(info);
  }

  /**
   * @brief Return the engine the uri is in.
   * @param ddfURI The ddf uri.
   * @return The engine name.
   */
  public String getEngineNameOfDDF(String ddfURI) throws DDFException {
    // String[] stringArrays = ddfURI.split("/");
    // Test here.
    // if (stringArrays.length <= 2) {
    //   throw new DDFException("Can't get engine from the uri");
    // }
    // return stringArrays[2];
    DDFManager manager = null;
    if (this.mDDFCoordinator == null) {
      manager = this;
    } else {
      manager = this.mDDFCoordinator.getDDFManagerByURI(ddfURI);
    }
    if (manager == null) {
      manager.log("Can't get manager for " + ddfURI);
    }
    return manager.getEngineName();
  }

  /**
   * @brief Tranfer the ddf from another engine. This should be override by
   * subclass.
   * @param fromEngine The engine where the ddf is.
   * @param ddfuri The ddfuri.
   * @return The new ddf.
   */
  public abstract DDF transfer(String fromEngine, String ddfuri) throws DDFException;

  public  DDF transferByTable(String fromEngine, String tableName)
          throws  DDFException { return null; }

  /**
   * List of existing DDFs
   */

  protected DDFCache mDDFCache = new DDFCache();

  protected Map<String, IModel> mModels = new ConcurrentHashMap<String, IModel>();

  public void addDDF(DDF ddf) throws DDFException {
    mDDFCache.addDDF(ddf);
    if (this.mDDFCoordinator != null) {
      mDDFCoordinator.setUuid2DDFManager(ddf.getUUID(), this);
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

  public DDF getDDF(UUID uuid) throws DDFException {
    return mDDFCache.getDDF(uuid);
  }

  public boolean hasDDF(UUID uuid) {
    return mDDFCache.hasDDF(uuid);
  }

  public DDF getDDFByName(String name) throws DDFException {
    return mDDFCache.getDDFByName(name);
  }

  public synchronized void setDDFName(DDF ddf, String name) throws DDFException {
    mDDFCache.setDDFName(ddf, name);
    mLog.info("set ddfname : " + "ddf://" + ddf.getNamespace() + "/" + name);
    if (mDDFCoordinator != null) {
      mDDFCoordinator.setURI2DDFManager("ddf://" + ddf.getNamespace() + "/" +
              name, this);
    }
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

  public DDFManager(DataSourceDescriptor dataSourceDescriptor,
                    String engineName) {
    this.setEngineName(engineName);
    this.startup();
  }

  public DDFManager(String namespace) {
    this.setNamespace(namespace);
    this.startup();
  }

  public static DDFManager get(String engineType, DataSourceDescriptor dataSourceDescriptor)
          throws DDFException {
    if (Strings.isNullOrEmpty(engineType)) {
      engineType = ConfigConstant.ENGINE_NAME_DEFAULT.toString();
    }

    String className = Config.getValue(engineType, ConfigConstant
            .FIELD_DDF_MANAGER);
    // if (Strings.isNullOrEmpty(className)) return null;
    if (Strings.isNullOrEmpty(className)) {
      throw new DDFException("ERROR: in jdbc ddfmanger, class name is " + className +
              " when enginename is : " + engineType );
    }

    try {
      Class[] classType = new Class[2];
      classType[0] = DataSourceDescriptor.class;
      classType[1] = String.class;

      DDFManager manager = (DDFManager) Class.forName(className)
              .getDeclaredConstructor(classType).newInstance
                      (dataSourceDescriptor, engineType);
      return manager;
    } catch (Exception e) {
      //throw new DDFException("Cannot get DDFManager for engine " +
      //        engineType + " classname "
      //        + className + " " + e.getMessage());
      throw new DDFException(e);
    }
  }

  /**
   * Returns a new instance of {@link DDFManager} for the given engine name
   *
   * @param engineName
   * @return
   * @throws Exception
   */
  public static DDFManager get(String engineName) throws DDFException {
    if (Strings.isNullOrEmpty(engineName)) {
      engineName = ConfigConstant.ENGINE_NAME_DEFAULT.toString();
    }

    String className = Config.getValue(engineName, ConfigConstant.FIELD_DDF_MANAGER);
    // if (Strings.isNullOrEmpty(className)) return null;
    if (Strings.isNullOrEmpty(className)) {
      throw new DDFException("ERROR: in jdbc ddfmanger, class name is " + className +
      " when enginename is : " + engineName );
    }

    try {
      Class testclass = Class.forName(className);
      if (testclass == null) {
        throw new DDFException("test class is null");
      }
      DDFManager testmanager = (DDFManager) testclass.newInstance();
      if (testmanager == null) {
        throw new DDFException("ERROR: testmanager is null");
      }
      return testmanager;
      // return (DDFManager) Class.forName(className).newInstance();

    } catch (Exception e) {
      // throw new DDFException("Cannot get DDFManager for engine " + engineName, e);
      e.printStackTrace();
      throw new DDFException("Cannot get DDFManager for engine " + engineName + " classname "
              + className + " " + e.getMessage());

    }
  }

  private DDF mDummyDDF;

  protected DDF getDummyDDF() throws DDFException {
    if (mDummyDDF == null) mDummyDDF = this.newDDF(this);
    return mDummyDDF;
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF".
   *
   * @param manager
   * @param data
   * @param typeSpecs
   * @param namespace
   * @param name
   * @param schema
   * @return
   * @throws DDFException
   */
  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs,
                    String engineName, String namespace, String name, Schema
                            schema)
      throws DDFException {

    // @formatter:off
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
				Class[].class, String.class, String.class, String.class, Schema
                    .class },
				new Object[] { manager, data, typeSpecs, engineName,
                        namespace, name,
						schema });
    return ddf;
  }

  // TODO: For back compatability.
  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs,
                    String namespace, String name, Schema schema) throws DDFException {
    return this.newDDF(manager, data, typeSpecs, manager.getEngineName(),
            namespace, name, schema);
  }

  public DDF newDDF(Object data, Class<?>[] typeSpecs, String engineName,
                    String
                    namespace, String name, Schema schema)
      throws DDFException {

    // @formatter:off
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
						Class[].class, String.class, String.class, String
                    .class, Schema.class },
						new Object[] { this, data, typeSpecs, engineName,
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
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, null, null, null);
  }

  public DDF sql2ddf(String command, String dataSource) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, new SQLDataSourceDescriptor(null, dataSource, null, null, null));
  }

  public DDF sql2ddf(String command, DataSourceDescriptor dataSource) throws  DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, null, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataSourceDescriptor dataSource, DataFormat dataFormat) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.getDummyDDF().getSqlHandler().sql2ddfHandle(command, schema, dataSource, dataFormat);
  }


  @Override
  public SqlResult sql(String command) throws DDFException {
    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql(command, (Integer) null);
  }

  public SqlResult sql(String command, String dataSource) throws DDFException {

    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql(command, new SQLDataSourceDescriptor(null, dataSource,null, null, null));
  }

  @Override
  public SqlResult sql(String command, Integer maxRows) throws DDFException {

    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.sql(command, maxRows, null);
  }


  @Override
  public SqlResult sql(String command, Integer maxRows, DataSourceDescriptor dataSource) throws DDFException {

    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.getDummyDDF().getSqlHandler().sqlHandle(command, maxRows, dataSource);
  }

  public SqlResult sql(String command, DataSourceDescriptor dataSource) throws DDFException {

    mLog.info("Running command: " + command + " in Engine: " + this.getEngineName());
    return this.getDummyDDF().getSqlHandler().sqlHandle(command, null, dataSource);
  }



  @Override
  public SqlTypedResult sqlTyped(String command) throws DDFException {
    return this.sqlTyped(command, null);
  }

  @Override
  public SqlTypedResult sqlTyped(String command, Integer maxRows) throws DDFException {
    return this.sqlTyped(command, maxRows, null);
  }

  @Override
  public SqlTypedResult sqlTyped(String command, Integer maxRows, DataSourceDescriptor dataSource) throws DDFException {
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
    if (Strings.isNullOrEmpty(uri.getEngine())) throw new DDFException("Engine/Protocol in URI cannot be missing");
    return DDFManager.get(uri.getEngine()).load(uri);
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
}
