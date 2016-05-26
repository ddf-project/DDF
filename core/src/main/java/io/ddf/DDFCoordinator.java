package io.ddf;


import com.google.common.base.Strings;
import io.ddf.DDFManager.EngineType;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ALoggable;
import io.ddf.misc.Config;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Created by jing on 7/23/15.
 */
public class DDFCoordinator extends ALoggable {
  // The mapping from ddf uuid to ddfmanager.
  protected Map<UUID, DDFManager> mDDFUUID2DDFManager
          = new ConcurrentHashMap<UUID, DDFManager>();
  // The mapping from engine uuid to ddfmanager
  protected Map<UUID, DDFManager> mEngineUUID2DDFManager
          = new ConcurrentHashMap<UUID, DDFManager>();
  // The default engine.
  protected DDFManager mDefaultEngine;

  protected String mNamespace = null;

  public DDFManager getDefaultEngine() {
    return mDefaultEngine;
  }

  /**
   * @brief Remove an engine from our system. We should
   *        (1) stop the engine from running, for example, stop the jdbc
   *        conncetion.
   *        (2) remove the associated structures, including the manager
   *        structure and also related ddf strucutres.
   * @param engineUUID The uuid of the engine
   * @throws DDFException
   */
  public void removeEngine(UUID engineUUID) throws DDFException {
    if (!mEngineUUID2DDFManager.containsKey(engineUUID)) {
      throw new DDFException("There is no engine with UUID : " + engineUUID);
    }
    DDFManager manager = mEngineUUID2DDFManager.get(engineUUID);
    mEngineUUID2DDFManager.remove(engineUUID);
    for (DDF ddf : manager.listDDFs()) {
      UUID uuid = ddf.getUUID();
      // TODO: Recheck here? What if thd ddf doesn't have uri?
      String uri = ddf.getUri();
      mDDFUUID2DDFManager.remove(uuid);
    }
  }
  
  public String getNamespace() throws DDFException {
    if (Strings.isNullOrEmpty(mNamespace)) {
      mNamespace = Config.getValueWithGlobalDefault(Config.ConfigConstant
              .SECTION_GLOBAL.toString(),
          Config.ConfigConstant.FIELD_NAMESPACE);
    }

    return mNamespace;
  }


  /**
   * @brief Restore the engines after restarting.
   */
  public void restoreEngines() {}

  /**
   * @brief Constructor.
   */
  public DDFCoordinator() {}

  /**
   * @brief Add the association from ddf uuid to ddfmanager.
   * @param uuid The uuid of the ddf.
   * @param ddfManager The ddfmanager.
   */
  public void setDDFUUID2DDFManager(UUID uuid, DDFManager ddfManager) {
    mDDFUUID2DDFManager.put(uuid, ddfManager);
  }

  /**
   * @brief Get ddfmanager by uuid.
   * @param uuid The uuid of the ddf.
   * @return The ddfmanager.
   * @throws DDFException
   */
  public DDFManager getDDFManagerByDDFUUID(UUID uuid) throws DDFException {
    DDFManager manager = mDDFUUID2DDFManager.get(uuid);
    if (manager == null) {
      mLog.info(">>> Can't get DDFManager for uuid " + uuid.toString());
      throw new DDFException("Can't get DDFManager for uuid: " + uuid.toString());
    }
    return manager;
  }

  /**
   * @brief Set default compute engine.
   * @param defaultEngine The default compute engine.
   */
  public void setDefaultEngine(DDFManager defaultEngine) {
    this.mDefaultEngine = defaultEngine;
  }

  /**
   * @brief Add a new manager to the system. Include
   *        (1) Create the structure mapping for the ddfmanager.
   *        (2) Create the structure mapping for the ddfs.
   * @param manager The ddfmanager.
   */
  public synchronized void addComputeEngine(DDFManager manager) {
    this.mEngineUUID2DDFManager.put(manager.getUUID(), manager);
    DDF[] ddfs = manager.listDDFs();
    for(DDF ddf: ddfs) {
      mDDFUUID2DDFManager.put(ddf.getUUID(), manager);
    }
  }

  /**
   * @brief Add ddf to the system.
   * @param ddf The ddf.
   */
  public synchronized void addDDF(DDF ddf) {
    if(this.mEngineUUID2DDFManager.get(ddf.getManager().getUUID()) == null) {
      this.addComputeEngine(ddf.getManager());
    }
    this.mDDFUUID2DDFManager.put(ddf.getUUID(), ddf.getManager());
  }


  /**
   * @brief Show all the engines in the system.
   * @return List of engine uuids.
   */
  public List<UUID> showEngines() {
    List<UUID> ret = new ArrayList<UUID>();
    for (UUID uuid : mEngineUUID2DDFManager.keySet()) {
      ret.add(uuid);
    }
    return ret;
  }


  /**
   * @brief Get DDF by uuid.
   * @param uuid The uuid of the ddf.
   * @return The ddf.
   * @throws DDFException
   */
  public DDF getDDF(UUID uuid) throws DDFException {
    DDFManager manager = mDDFUUID2DDFManager.get(uuid);

    // if we know the manager of given DDF then use it to get the DDF
    if (manager != null) {
      return manager.getDDF(uuid);
    }

    // otherwise, try to find it in all known DDFManagers
    for (Map.Entry<UUID, DDFManager> entry: mEngineUUID2DDFManager.entrySet()) {
      manager = entry.getValue();
      try {
        return manager.getDDF(uuid);
      } catch (DDFException ignored) {
        // this is not the right manager for given DDF id
      }
    }

    throw new DDFException("Can't find ddf with uuid: " + uuid.toString());
  }

  /**
   * @brief Return the uuid for one uri.
   * @param uri The uri of the ddf.
   * @return The uuid of the ddf.
   */
  public UUID uuidFromUri(String uri) throws DDFException {
    for (Map.Entry<UUID, DDFManager> entry : mDDFUUID2DDFManager.entrySet()) {
      DDFManager ddfmanager = entry.getValue();
      try {
        DDF ddf = ddfmanager.getDDFByURI(uri);
        return ddf.getUUID();
      } catch (DDFException e) {
        // e.printStackTrace();
      }
    }
    throw new DDFException("Can't find ddf with uri: " + uri);
  }

  public DDFManager initEngine(EngineType engineType) throws DDFException {
    return this.initEngine(engineType, null);
  }


  /**
   * @brief Init an engine.
   * @param engineUUID The uuid of the engine, if it's null, it will be
   *                   randomly picked.
   * @param engineType The type of the engine.
   * @param dataSourceDescriptor The descriptor of the datasource.
   * @return The ddfmanager.
   * @throws DDFException
   */
  public DDFManager initEngine(UUID engineUUID,
                               EngineType engineType,
                               DataSourceDescriptor dataSourceDescriptor)
          throws DDFException {
    DDFManager manager = (null != dataSourceDescriptor)
                         ? DDFManager.get(engineType, dataSourceDescriptor)
                         : DDFManager.get(engineType);
    if (manager == null) {
      throw new DDFException("Error int get the DDFManager for engine :" + engineType);
    }
    if (engineUUID != null) {
      manager.setUUID(engineUUID);
    }
    manager.setEngineType(engineType);
    manager.setDDFCoordinator(this);
    mEngineUUID2DDFManager.put(manager.getUUID(), manager);
    return manager;
  }


  /**
   * @param engineType           The type of the engine.
   * @param dataSourceDescriptor DataSource.
   * @return The new ddf manager.
   * @throws DDFException
   * @brief Init an engine.
   */
  public DDFManager initEngine(EngineType engineType, DataSourceDescriptor dataSourceDescriptor) throws DDFException {
    return this.initEngine(null, engineType, dataSourceDescriptor);
  }

  /**
   * @param engineUUID The uuid of the engine.
   * @return The ddfmanager, or null if there is no such engine.
   * @throws DDFException
   * @brief Get the engine with given name.
   */
  public DDFManager getEngine(UUID engineUUID) throws DDFException {
    DDFManager manager = mEngineUUID2DDFManager.get(engineUUID);
    if (manager == null) {
      throw new DDFException("There is no engine with name : " + engineUUID);
    }
    return manager;
  }

}
