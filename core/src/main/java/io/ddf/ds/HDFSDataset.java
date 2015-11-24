package io.ddf.ds;


import io.ddf.datasource.DataSourceSchema;
import io.ddf.datasource.FileFormat;

import java.net.URI;
import java.util.UUID;

/**
 */
public class HDFSDataset extends TextFileDataset {

  public HDFSDataset(UUID id, UUID dataSourceId, URI uri, DataSourceSchema schema, FileFormat fileFormat) {
    super(id, dataSourceId, uri, schema, fileFormat);
  }
}
