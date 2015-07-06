package io.ddf.datasource;


import io.ddf.content.Schema;

import java.util.List;

/**
 */
public class DataSourceSchema {
  private List<Schema.Column> mColumns;

  public DataSourceSchema(List<Schema.Column> columns) {
    this.mColumns = columns;
  }

  public void setColumns(List<Schema.Column> columns) {
    this.mColumns = columns;
  }

  public List<Schema.Column> getColumns() {
    return this.mColumns;
  }
}
