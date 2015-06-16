package io.ddf.datasource;


import io.ddf.DDF;
import io.ddf.DDFManager;

/**
 */

abstract public class DataSourceDescriptor {

  private DataSourceSchema mDataSourceSchema;

  private DataSourceURI mDataSourceUri;

  private IDataSourceCredentials mDataSourceCredentials;

  public DataSourceDescriptor(DataSourceURI dataSourceURI, IDataSourceCredentials dataSourceCredentials,
      DataSourceSchema dataSourceSchema) {
    this.mDataSourceUri = dataSourceURI;
    this.mDataSourceCredentials = dataSourceCredentials;
    this.mDataSourceSchema = dataSourceSchema;
  }


  public void setDataSourceSchema(DataSourceSchema dataSourceSchema) {
    this.mDataSourceSchema = dataSourceSchema;
  }

  public DataSourceSchema getDataSourceSchema() {
    return this.mDataSourceSchema;
  }

  public void setDataSourceUri(DataSourceURI dataSourceUri) {
    this.mDataSourceUri = dataSourceUri;
  }

  public DataSourceURI getDataSourceUri() {
    return this.mDataSourceUri;
  }

  public void setDataSourceCredentials(IDataSourceCredentials dataSourceCredentials) {
    this.mDataSourceCredentials = dataSourceCredentials;
  }

  public IDataSourceCredentials getDataSourceCredentials() {
    return this.mDataSourceCredentials;
  }

  public abstract DDF load(DDFManager manager);
}
