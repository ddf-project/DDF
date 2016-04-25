/**
 *
 */
package io.ddf.content;


import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.SQLDataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import org.apache.avro.generic.GenericData;

import java.util.*;

public abstract class AMetaDataHandler extends ADDFFunctionalGroupHandler
    implements IHandleMetaData {


  public AMetaDataHandler(DDF theDDF) {
    super(theDDF);
  }

  private UUID mId = UUID.randomUUID();

  @Override
  public UUID getId() {
    return mId;
  }

  @Override
  public void setId(UUID id) {
    mId = id;
  }

  private long mNumRows = 0L;
  private boolean bNumRowsIsValid = false;
  private int useCount = 0;
  private DataSourceDescriptor mDataSourceDescriptor;
  private Date mLastRefreshTime;
  private String mLastRefreshUser;
  private Date mLastModifiedTime;
  private String mLastModifiedUser;
  private Date mLastPersistedTime;
  private DataSourceDescriptor mSnapshotDescriptor;
  private Map<String, Map<String, Integer>> mLevelCounts = new HashMap<String, Map<String, Integer>>();
  /**
   * Each implementation needs to come up with its own way to compute the row
   * count.
   *
   * @return row count of a DDF
   */
  // protected abstract long getNumRowsImpl() throws DDFException;

  /**
   * Called to assert that the row count needs to be recomputed at next access
   */

  protected void invalidateNumRows() {
    bNumRowsIsValid = false;
  }

  @Override
  public long getNumRows() throws DDFException {
    if (!bNumRowsIsValid) {
      mNumRows = this.getNumRowsImpl();
      //      bNumRowsIsValid = true;
    }
    return mNumRows;
  }

  protected long getNumRowsImpl() throws DDFException {
    this.mLog.debug("get NumRows Impl called");
    try {
      String sqlcmd = "SELECT COUNT(*) FROM {1}";
      List<String> rs = this.getManager().sql(sqlcmd,
          new SQLDataSourceDescriptor(sqlcmd, null, null, null, this
              .getDDF().getUUID().toString())).getRows();
      return Long.parseLong(rs.get(0));
    } catch (DDFException e) {
      throw e;
    } catch (Exception e) {
      throw new DDFException("Error getting NRow", e);
    }
  }

  /**
   * Transfer factor information from ddf to this DDF
   * @param ddf
   * @throws DDFException
   */
  public void copyFactor(DDF ddf)  throws DDFException {
    this.copyFactor(ddf, null);
  }

  /**
   * Transfer factor information from ddf to this DDF
   * @param ddf
   * @param columns Columns to re-compute factors
   * @throws DDFException
   */
  public void copyFactor(DDF ddf, List<String> columns)  throws DDFException {

    // if there is no columns to recompute factor info
    if (columns == null) {
      columns = new ArrayList<String>();
    }

    for (Schema.Column col : ddf.getSchema().getColumns()) {
      if (this.getDDF().getColumn(col.getName()) != null && col.getColumnClass() == Schema.ColumnClass.FACTOR) {
        // Set corresponding column as factor
        this.getDDF().getSchemaHandler().setAsFactor(col.getName());
        // if not in list of columns to re-compute factors
        // then we just copy existing factor info to the new ones
        if (!columns.contains(col.getName())) {
          // copy existing factor column info
          this.getDDF().getSchemaHandler().setFactorLevels(col.getName(), col.getOptionalFactor());
        }
      }
    }
  }

  public void copy(IHandleMetaData fromMetaData) throws DDFException {
    if(fromMetaData instanceof AMetaDataHandler) {
      AMetaDataHandler metaDataHandler = (AMetaDataHandler) fromMetaData;
      this.mLastRefreshTime = metaDataHandler.getLastRefreshTime();
      this.mLastRefreshUser = metaDataHandler.getLastRefreshUser();
      this.mLastModifiedTime = metaDataHandler.getLastModifiedTime();
      this.mLastModifiedUser = metaDataHandler.getLastModifiedUser();
    }
    this.copyFactor(fromMetaData.getDDF());
  }

  private HashMap<Integer, ICustomMetaData> mCustomMetaDatas;

  public ICustomMetaData getCustomMetaData(int idx) {
    return mCustomMetaDatas.get(idx);
  }

  public void setCustomMetaData(ICustomMetaData customMetaData) {
    mCustomMetaDatas.put(customMetaData.getColumnIndex(), customMetaData);
  }

  public HashMap<Integer, ICustomMetaData> getListCustomMetaData() {
    return mCustomMetaDatas;
  }

  public static interface ICustomMetaData {

    public double[] buildCoding(String value);

    public double get(String value, int idx);

    public int getColumnIndex();
  }

  public void setDataSourceDescriptor(DataSourceDescriptor dataSource) {
    this.mDataSourceDescriptor = dataSource;
  }

  public DataSourceDescriptor getDataSourceDescriptor() {
    return this.mDataSourceDescriptor;
  }

  public void setLastRefreshTime(Date time) {
    this.mLastRefreshTime = time;
  }

  public Date getLastRefreshTime() {
    return this.mLastRefreshTime;
  }

  public void setLastRefreshUser(String user) {
    this.mLastRefreshUser = user;
  }

  public String getLastRefreshUser() {
    return this.mLastRefreshUser;
  }

  public void setLastModifiedTime(Date time) {
    this.mLastModifiedTime = time;
  }

  public Date getLastModifiedTime() {
    return this.mLastModifiedTime;
  }

  public void setLastModifiedUser(String user) {
    this.mLastModifiedUser = user;
  }

  public String getLastModifiedUser() {
    return this.mLastModifiedUser;
  }

  public void setLastPersistedTime(Date time) {
    this.mLastPersistedTime = time;
  }

  public Date getLastPersistedTime() {
    return this.mLastPersistedTime;
  }

  public void setSnapshotDescriptor(DataSourceDescriptor snapshot) {
    this.mSnapshotDescriptor = snapshot;
  }

  public DataSourceDescriptor getSnapshotDescriptor() {
    return this.mSnapshotDescriptor;
  }


  @Override
  public void setLevelCounts(Map<String, Map<String, Integer>> levelCounts) throws DDFException {
    Map<String, Map<String, Integer>> currentLevelCounts = this.getLevelCounts();
    currentLevelCounts.putAll(levelCounts);
  }

  @Override
  public void removeLevelCounts() throws DDFException {
    this.mLevelCounts = new HashMap<String, Map<String, Integer>>();
  }

  @Override
  public void removeLevelCountsForColumn(String columnName) {
    this.mLevelCounts.remove(columnName);
  }

  @Override
  public Map<String, Map<String, Integer>> getLevelCounts() throws DDFException {
    return this.mLevelCounts;
  }
}
