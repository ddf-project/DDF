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
import io.ddf.content.APersistenceHandler.PersistenceUri;
import io.ddf.content.IHandlePersistence.IPersistible;
import io.ddf.content.IHandleRepresentations;
import io.ddf.content.Schema;
import io.ddf.content.Schema.DataFormat;
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
import scala.tools.jline.internal.Log;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
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

  /**
   * List of existing DDFs
   */
  protected Map<String, DDF> mDDFs = new ConcurrentHashMap<String, DDF>();

  protected Map<String, IModel> mModels = new ConcurrentHashMap<String, IModel>();

  public void addDDF(DDF ddf) throws DDFException {
    mDDFs.put(ddf.getUUID(), ddf);
  }

  public void removeDDF(DDF ddf) throws DDFException {
    mDDFs.remove(ddf.getUUID());
  }

  public DDF[] listDDFs() {
    return this.mDDFs.values().toArray(new DDF[] {});
  }

  public DDF getDDF(String uuid) {
    DDF data = mDDFs.get(uuid);
    return data;
  }

  public DDF getDDFByName(String name) throws DDFException {
    DDF result= null;
    for(DDF ddf: mDDFs.values()) {
      if(!Strings.isNullOrEmpty(ddf.getName()) && ddf.getName().equals(name)) {
        result = ddf;
      }
    }
    if(result == null) throw new DDFException(String.format("Cannot find ddf with name %s", name));
    else {
      return result;
    }
  }

  public synchronized void setDDFName(DDF ddf, String name) throws DDFException {
    ddf.setName(name);
  }

  public synchronized void setDDFUUID(DDF ddf, String uuid) throws DDFException{
    DDF existedDDF = this.getDDF(uuid);
    if(existedDDF != null) {
      throw new DDFException(String.format("DDF with uuid % already exists", uuid));
    }
    ddf.setUUID(uuid);
  }

  public DDF getDDFByURI(String uri) throws DDFException {
    DDF result = null;
    for(DDF ddf: mDDFs.values()) {
      if(!Strings.isNullOrEmpty(ddf.getUri()) && ddf.getUri().equals(uri)) {
        result = ddf;
      }
    }
    if(result == null) throw new DDFException(String.format("Cannot find ddf with uri %s", uri));
    else {
      return result;
    }
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

  public DDFManager(String namespace) {
    this.setNamespace(namespace);
    this.startup();
  }

  /**
   * Returns a new instance of {@link DDFManager} for the given engine name
   *
   * @param engineName
   * @return
   * @throws Exception
   */
  public static DDFManager get(String engineName) throws DDFException {
    if (Strings.isNullOrEmpty(engineName)) engineName = ConfigConstant.ENGINE_NAME_DEFAULT.toString();

    String className = Config.getValue(engineName, ConfigConstant.FIELD_DDF_MANAGER);
    if (Strings.isNullOrEmpty(className)) return null;

    DDFManager manager;
    try {
      manager = (DDFManager) Class.forName(className).newInstance();

    } catch (Exception e) {
      throw new DDFException("Cannot get DDFManager for engine " + engineName, e);
    }

    return manager;
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
  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {

    // @formatter:off
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
				Class[].class, String.class, String.class, Schema.class },
				new Object[] { manager, data, typeSpecs, namespace, name,
						schema });
    return ddf;
  }

  public DDF newDDF(Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema)
      throws DDFException {

    // @formatter:off
    DDF ddf = this.newDDF(new Class<?>[] { DDFManager.class, Object.class,
						Class[].class, String.class, String.class, Schema.class },
						new Object[] { this, data, typeSpecs, namespace, name,
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
    return this.newDDF(new Class<?>[] { DDFManager.class }, new Object[] { manager });
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF", passing in this DDFManager as the sole argument
   *
   * @return the newly instantiated DDF
   * @throws DDFException
   */
  public DDF newDDF() throws DDFException {
    return this.newDDF(new Class<?>[] { DDFManager.class }, new Object[] { this });
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
      throw new DDFException(String.format(
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

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    return this.getDummyDDF().getSqlHandler().sql2ddf(command, schema, dataSource, dataFormat);
  }

  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.sql2txt(command, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows) throws DDFException {
    return this.sql2txt(command, maxRows, null);
  }

  @Override
  public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
    return this.getDummyDDF().getSqlHandler().sql2txt(command, maxRows, dataSource);
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

  public abstract DDF loadTable(String fileURL, String fieldSeparator) throws DDFException;

}
