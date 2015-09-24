package io.ddf;


import com.google.common.base.Strings;
import io.ddf.DDFManager.EngineType;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.DDFManager.EngineType;
import io.ddf.misc.ALoggable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Created by jing on 7/23/15.
 */
public class DDFCoordinator extends ALoggable {
  // The ddfmangers.
  private List<DDFManager> mDDFManagerList = new ArrayList<DDFManager>();
  // The mapping from engine name to ddfmanager.
  private Map<UUID, DDFManager> mDDFUuid2DDFManager = new ConcurrentHashMap<UUID, DDFManager>();
  private Map<String, DDFManager> mDDFURI2DDFManager = new ConcurrentHashMap<String, DDFManager>();
  private Map<UUID, DDFManager> mUUID2DDFManager = new ConcurrentHashMap<UUID, DDFManager>();
  // The default engine.
  private DDFManager mDefaultEngine;
  private String mComputeEngine;


  public DDFManager getDefaultEngine() {
    return mDefaultEngine;
  }

  public void removeEngine(UUID engineUUID) throws DDFException {
    if (!mUUID2DDFManager.containsKey(engineUUID)) {
      throw new DDFException("There is no engine with UUID : " + engineUUID);
    }
    DDFManager manager = mUUID2DDFManager.get(engineUUID);
    mUUID2DDFManager.remove(engineUUID);
    mDDFManagerList.remove(manager);
    for (DDF ddf : manager.listDDFs()) {
      UUID uuid = ddf.getUUID();
      String uri = ddf.getUri();
      mDDFUuid2DDFManager.remove(uuid);
      mDDFURI2DDFManager.remove(uri);
    }
  }

  public void restoreEngines() {}

  public DDFCoordinator() {}
  
  public void setURI2DDFManager(String uri, DDFManager ddfManager) {
    ddfManager.log("Set uri2ddfManager with uri: " + uri);
    mDDFURI2DDFManager.put(uri, ddfManager);
  }

  public DDFManager getDDFManagerByURI(String uri) throws DDFException {
    DDFManager dm = mDDFURI2DDFManager.get(uri);
    if (dm == null) {

      throw new DDFException("Can't find ddfmanager for ddf: " +
          uri + " after trying spark ");

    }
    return dm;
  }

  public void setUuid2DDFManager(UUID uuid, DDFManager ddfManager) {
    mDDFUuid2DDFManager.put(uuid, ddfManager);
  }

  public DDFManager getDDFManagerByUUID(UUID uuid) throws DDFException {
    DDFManager manager = mDDFUuid2DDFManager.get(uuid);
    if (manager == null) {
      mLog.info(">>> ddfManager is null for uuid " + uuid.toString());
      throw new DDFException("Can't get DDFManager for uuid: " + uuid.toString());
    }
    return manager;
  }

  public void setDefaultEngine(DDFManager defaultEngine) {
    this.mDefaultEngine = defaultEngine;
  }

  public synchronized void addComputeEngine(DDFManager manager) {
    this.mDDFManagerList.add(manager);
    this.mUUID2DDFManager.put(manager.getUUID(), manager);
    DDF[] ddfs = manager.listDDFs();
    for(DDF ddf: ddfs) {
      mDDFUuid2DDFManager.put(ddf.getUUID(), manager);
      if(!Strings.isNullOrEmpty(ddf.getUri())) {
        mDDFURI2DDFManager.put(ddf.getUri(), manager);
      }
    }
  }

  public synchronized void addDDF(DDF ddf) {
    if(this.mUUID2DDFManager.get(ddf.getManager().getUUID()) == null) {
      this.addComputeEngine(ddf.getManager());
    }
    this.mDDFUuid2DDFManager.put(ddf.getUUID(), ddf.getManager());
    if(!Strings.isNullOrEmpty(ddf.getUri())) {
      this.mDDFURI2DDFManager.put(ddf.getUri(), ddf.getManager());
    }
  }

  public List<DDFManager> getDDFManagerList() {
    return mDDFManagerList;
  }

  public List<UUID> showEngines() {
    List<UUID> ret = new ArrayList<UUID>();
    for (DDFManager ddfManager : this.mDDFManagerList) {
      ret.add(ddfManager.getUUID());
    }
    return ret;
  }

  public DDF getDDF(UUID uuid) throws DDFException {
    for (DDFManager ddfManager : mDDFManagerList) {
      try {
        DDF ddf = ddfManager.getDDF(uuid);
        return ddf;
      } catch (DDFException e) {
        // e.printStackTrace();
      }
    }
    throw new DDFException("Can't find ddf with uuid: " + uuid.toString());
  }

  public DDF getDDFByURI(String uri) throws DDFException {
    for (DDFManager ddfManager : mDDFManagerList) {
      try {
        DDF ddf = ddfManager.getDDFByURI(uri);
        return ddf;
      } catch (Exception e) {
        try {
          DDF ddf = ddfManager.getOrRestoreDDFUri(uri);
          return ddf;
        } catch (Exception e2) {
          throw new DDFException("Can't find ddf with uri: " + uri, e2);
        }
      }
    }
    throw new DDFException("Can't find ddf with uri: " + uri);
  }

  public DDFManager initEngine(EngineType engineType) throws DDFException {
    return this.initEngine(engineType, null);
  }

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
    mDDFManagerList.add(manager);
    mUUID2DDFManager.put(manager.getUUID(), manager);
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

  public int stopEngine(String engineName) {
    return 0;
  }

  /**
   * @param engineUUID the engine uuid.
   * @return The "show tables" result.
   * @throws DDFException
   * @brief Browse what content is in the engine.
   */
  public SqlResult browseEngine(UUID engineUUID) throws DDFException {
    DDFManager ddfManager = this.getEngine(engineUUID);
    return ddfManager.sql("show tables", ddfManager.getEngine());
  }

  /**
   * @return
   * @throws DDFException
   * @brief Browse ddfs in all engines.
   */
  public List<SqlResult> browseEngines() throws DDFException {
    List<SqlResult> retList = new ArrayList<SqlResult>();
    for (UUID engineUUID : mUUID2DDFManager.keySet()) {
      retList.add(this.browseEngine(engineUUID));
    }
    return retList;
  }

  /**
   * @param engineUUID The uuid of the engine.
   * @return The ddfmanager, or null if there is no such engine.
   * @throws DDFException
   * @brief Get the engine with given name.
   */
  public DDFManager getEngine(UUID engineUUID) throws DDFException {
    DDFManager manager = mUUID2DDFManager.get(engineUUID);
    if (manager == null) {
      throw new DDFException("There is no engine with name : " + engineUUID);
    }
    return manager;
  }

  public DDF sql2ddf(String sqlCmd, UUID engineUUID) {
    return null;
  }

  public DDF sql2ddf(String sqlCmd, DataSourceDescriptor dataSourceDescriptor) {
    return null;
  }

  /**
   * @param sqlcmd The command.
   * @return The sql result.
   * @brief Run the sql command using default compute engine.
   */
  public SqlResult sqlX(String sqlcmd) throws DDFException {
    if (this.getDefaultEngine() == null) {
      throw new DDFException(
          "No default engine specified, please " + "specify the default engine or pass the engine name");
    }
    return this.sqlX(sqlcmd, this.getDefaultEngine().getUUID());
  }

  /**
   * @param sqlcmd
   * @param engineUUID
   * @return
   * @brief Run the sql command on the given enginename.
   */
  public SqlResult sqlX(String sqlcmd, UUID engineUUID) throws DDFException {
    DDFManager defaultManager = this.getEngine(engineUUID);
    return defaultManager.sql(sqlcmd);
  }

  public DDF sql2ddfX(String sqlcmd) throws DDFException {
    if (this.getDefaultEngine() == null) {
      throw new DDFException(
          "No default engine specified, please " + "specify the default engine or pass the engine name");
    }
    return this.sql2ddf(sqlcmd, this.getDefaultEngine().getUUID());
  }

  public DDF sql2ddfX(String sqlcmd, UUID engineUUID) throws DDFException {
    DDFManager defaultManager = this.getEngine(engineUUID);
    return defaultManager.sql2ddf(sqlcmd);
  }

  public DDF transfer(UUID fromEngine, UUID engineUUID, String ddfuri) throws DDFException {
    DDFManager defaultManager = this.getEngine(engineUUID);
    return defaultManager.transfer(fromEngine, ddfuri);
  }
}
