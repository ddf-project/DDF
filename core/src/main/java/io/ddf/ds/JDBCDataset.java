package io.ddf.ds;


import java.net.URI;
import java.util.UUID;

/**
 */
public class JDBCDataset extends DataSet {

  private String tableName;

  public JDBCDataset(UUID id, UUID dataSourceId, URI uri, String tableName) {
    super(id, dataSourceId, uri);
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
