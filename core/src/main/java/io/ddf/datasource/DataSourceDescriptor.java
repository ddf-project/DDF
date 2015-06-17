package io.ddf.datasource;


import io.ddf.DDF;
import io.ddf.DDFManager;

/**
 */

abstract public class DataSourceDescriptor {

  private DataSourceSchema mDataSourceSchema;

  private IDataSourceURI mIDataSourceUri;

  private IDataSourceCredentials mDataSourceCredentials;

  private FileFormat mFileFormat;

  public DataSourceDescriptor(IDataSourceURI IDataSourceURI, IDataSourceCredentials dataSourceCredentials,
      DataSourceSchema dataSourceSchema, FileFormat fileFormat) {
    this.mIDataSourceUri = IDataSourceURI;
    this.mDataSourceCredentials = dataSourceCredentials;
    this.mDataSourceSchema = dataSourceSchema;
    this.mFileFormat = fileFormat;
  }


  public void setDataSourceSchema(DataSourceSchema dataSourceSchema) {
    this.mDataSourceSchema = dataSourceSchema;
  }

  public DataSourceSchema getDataSourceSchema() {
    return this.mDataSourceSchema;
  }

  public void setDataSourceUri(IDataSourceURI IDataSourceUri) {
    this.mIDataSourceUri = IDataSourceUri;
  }

  public IDataSourceURI getDataSourceUri() {
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

  public abstract DDF load(DDFManager manager);
}
