package io.ddf;


import io.ddf.DDFManager.EngineType;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.DDFManager.EngineType;
import java.util.*;

/**
 * Created by jing on 7/23/15.
 */
public class DDFCoordinator {
  // The ddfmangers.
  private List<DDFManager> mDDFManagerList = new ArrayList<DDFManager>();
  // The mapping from engine name to ddfmanager.
  private Map<UUID, DDFManager> mDDFUuid2DDFManager = new HashMap<UUID, DDFManager>();
  private Map<String, DDFManager> mURI2DDFManager = new HashMap<String, DDFManager>();
  private Map<UUID, DDFManager> mUUID2DDFManager = new HashMap<UUID, DDFManager>();
  // The default engine.
  private UUID mDefaultEngine;
  private String mComputeEngine;


  public UUID getDefaultEngine() {
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
      mURI2DDFManager.remove(uri);
    }
  }

  public void setURI2DDFManager(String uri, DDFManager ddfManager) {
    ddfManager.log("Set uri2ddfManager with uri: " + uri);
    mURI2DDFManager.put(uri, ddfManager);
  }

  public DDFManager getDDFManagerByURI(String uri) throws DDFException {
    DDFManager dm = mURI2DDFManager.get(uri);
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
      throw new DDFException("Can't get DDFManager for uuid: " + uuid.toString());
    }
    return manager;
  }

  public void setDefaultEngine(UUID defaultEngine) {
    this.mDefaultEngine = defaultEngine;
  }

  public String getComputeEngine() {
    return mComputeEngine;
  }

  public void setComputeEngine(String computeEngine) {
    mComputeEngine = computeEngine;
  }

  public List<DDFManager> getDDFManagerList() {
    return mDDFManagerList;
  }

  public void setDDFManagerList(List<DDFManager> mDDFManagerList) {
    this.mDDFManagerList = mDDFManagerList;
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

        }
      }
    }
    throw new DDFException("Can't find ddf with uri: " + uri);
  }

  /**
   * @param engineType           The type of the engine.
   * @param dataSourceDescriptor DataSource.
   * @return The new ddf manager.
   * @throws DDFException
   * @brief Init an engine.
   */
  public DDFManager initEngine(EngineType engineType, DataSourceDescriptor dataSourceDescriptor) throws DDFException {


    DDFManager manager = DDFManager.get(engineType, dataSourceDescriptor);
    if (manager == null) {
      throw new DDFException("Error int get the DDFManager for engine :" + engineType);
    }
    manager.setEngineType(engineType);
    manager.setDDFCoordinator(this);
    mDDFManagerList.add(manager);
    mUUID2DDFManager.put(manager.getUUID(), manager);
    return manager;
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
    return this.sqlX(sqlcmd, this.getDefaultEngine());
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
    return this.sql2ddf(sqlcmd, this.getDefaultEngine());
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
