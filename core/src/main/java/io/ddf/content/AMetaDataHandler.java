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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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
    } catch (Exception e) {
      throw new DDFException("Error getting NRow", e);
    }
  }

  @Override
  public boolean inUse() {
    if(useCount > 1) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized void increaseUseCount() {
    this.useCount += 1;
  }

  /**
   * Transfer factor information from ddf to this DDF
   * @param ddf
   * @throws DDFException
   */
  public void copyFactor(DDF ddf)  throws DDFException {
    for (Schema.Column col : ddf.getSchema().getColumns()) {
      this.getManager().log("colname is : " + col.getName());
      this.getManager().log("checking columns");
      this.getManager().log("ddf uuid: " + ddf.getUUID().toString());
      this.getManager().log("ddf uuid: " + this.getDDF().getUUID().toString());
      for (Schema.Column col2 : this.getDDF().getSchema().getColumns()) {
        this.getManager().log("col2: " + col2.getName() + " " + col2.getType
                ().toString());
      }
      if (this.getDDF().getColumn(col.getName()) != null && col.getColumnClass() == Schema.ColumnClass.FACTOR) {
        this.getDDF().getSchemaHandler().setAsFactor(col.getName());
      }
    }
    this.getDDF().getSchemaHandler().computeFactorLevelsAndLevelCounts();
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
}
