package io.ddf.ds;


import io.ddf.datasource.DataSourceSchema;
import io.ddf.datasource.FileFormat;

import java.net.URI;
import java.util.UUID;

/**
 */
public class TextFileDataset extends DataSet {

  private DataSourceSchema schema;

  private FileFormat fileFormat;

  public TextFileDataset(UUID id, UUID dataSourceId, URI uri, DataSourceSchema schema, FileFormat fileFormat) {
    super(id, dataSourceId, uri);
    this.schema = schema;
    this.fileFormat = fileFormat;
  }

  public DataSourceSchema getSchema() {
    return schema;
  }

  public void setSchema(DataSourceSchema schema) {
    this.schema = schema;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }
}
