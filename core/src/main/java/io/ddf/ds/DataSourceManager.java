package io.ddf.ds;


import io.ddf.DDF;
import io.ddf.exception.DDFException;

import java.util.List;
import java.util.UUID;

/**
 */
public abstract class DataSourceManager {

  public abstract DataSource getDataSource(UUID DataSourceId);

  public abstract void addDataSource(DataSource dataSource);

  public abstract DataSet getDataSet(UUID datasetId);

  public abstract void addDataSet(DataSet dataset);

  public abstract User getUser(String userId);

  public abstract void addUser(User user);

  public abstract DSUserCredentials getDSUserCredentials(UUID dsUSerCredentialsId);

  public abstract void addDSUserCredentials(DSUserCredentials credentials);

  public DDF load(User user, DataSet dataset) throws DDFException {
    DataSource dataSource = this.getDataSource(dataset.getDataSourceId());
    List<DSUserCredentials> credentialsList = dataSource.getDsUserCredentialsList();
    DSUserCredentials credentials = null;
    for(DSUserCredentials creds: credentialsList) {
      if(creds.getUserId().equals(user.getId()) && creds.getDataSourceId().equals(dataSource.getId())) {
        credentials = creds;
      }
    }

    if(credentials != null) {
      return this.loadImpl(dataset, dataSource, credentials);
    } else {
      throw new DDFException("Permission denied");
    }
  }

  protected abstract DDF loadImpl(DataSet dataset, DataSource dataSource, DSUserCredentials credentials);

  public DDF load(User user, DataSet dataset, DataSource dataSource, DSUserCredentials credentials) {
    return this.loadImpl(dataset, dataSource, credentials);
  }
}
