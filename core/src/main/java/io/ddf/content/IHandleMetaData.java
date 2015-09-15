package io.ddf.content;


import io.ddf.DDF;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.Date;
import java.util.UUID;

public interface IHandleMetaData extends IHandleDDFFunctionalGroup {

  public UUID getId();

  public void setId(UUID id);

  public long getNumRows() throws DDFException;

  public void copyFactor(DDF ddf)  throws DDFException;

  public void copy(IHandleMetaData otherMetaData) throws DDFException;

  public void setDataSourceDescriptor(DataSourceDescriptor dataSource);

  public DataSourceDescriptor getDataSourceDescriptor();

  public void setLastRefreshTime(Date time);

  public Date getLastRefreshTime();

  public void setLastRefreshUser(String user);

  public String getLastRefreshUser();

  public void setLastModifiedTime(Date time);

  public Date getLastModifiedTime();

  public void setLastModifiedUser(String user);

  public String getLastModifiedUser();

  public void setLastPersistedTime(Date time);

  public Date getLastPersistedTime();

  public void setSnapshotDescriptor(DataSourceDescriptor snapshotDescriptor);

  public DataSourceDescriptor getSnapshotDescriptor();
  // return true if ddf is in use
  // false otherwise
  public boolean inUse();

  //increase number of user using ddf
  public void increaseUseCount();
}
