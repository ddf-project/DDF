package io.ddf.datasource;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

/**
 */

public abstract class DataSourceDescriptor {

  private DataSourceSchema mDataSourceSchema;

  private DataSourceURI mIDataSourceUri;

  private IDataSourceCredentials mDataSourceCredentials;

  private FileFormat mFileFormat;

  public DataSourceDescriptor(DataSourceURI dataSourceURI, IDataSourceCredentials dataSourceCredentials,
      DataSourceSchema dataSourceSchema, FileFormat fileFormat) {
    this.mIDataSourceUri = dataSourceURI;
    this.mDataSourceCredentials = dataSourceCredentials;
    this.mDataSourceSchema = dataSourceSchema;
    this.mFileFormat = fileFormat;
  }

  public DataSourceDescriptor() {}

  public void setDataSourceSchema(DataSourceSchema dataSourceSchema) {
    this.mDataSourceSchema = dataSourceSchema;
  }

  public DataSourceSchema getDataSourceSchema() {
    return this.mDataSourceSchema;
  }

  public void setDataSourceUri(DataSourceURI dataSourceUri) {
    this.mIDataSourceUri = dataSourceUri;
  }

  public DataSourceURI getDataSourceUri() {
    return this.mIDataSourceUri;
  }

  public void setDataSourceCredentials(IDataSourceCredentials dataSourceCredentials) {
    this.mDataSourceCredentials = dataSourceCredentials;
  }

  public IDataSourceCredentials getDataSourceCredentials() {
    return this.mDataSourceCredentials;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.mFileFormat = fileFormat;
  }

  public FileFormat getFileFormat() {
    return this.mFileFormat;
  }

  public abstract DDF load(DDFManager manager) throws DDFException;
}
