package io.ddf.ds;


import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 */
public class SQLDataset extends DataSet {

  private String sqlCommand;

  private List<String> uriList;

  private List<String> uuidList;

  public SQLDataset(UUID id, UUID dataSourceId, URI uri, String sqlCommand, String dataSource, String nameSpace, List<String> uriList,
      List<String> uuidList) {
    super(id, dataSourceId, uri);
    this.sqlCommand = sqlCommand;
    this.uriList = uriList;
    this.uuidList = uuidList;
  }

  public String getSqlCommand() {
    return sqlCommand;
  }

  public void setSqlCommand(String sqlCommand) {
    this.sqlCommand = sqlCommand;
  }

  public List<String> getUriList() {
    return uriList;
  }

  public void setUriList(List<String> uriList) {
    this.uriList = uriList;
  }

  public List<String> getUuidList() {
    return uuidList;
  }

  public void setUuidList(List<String> uuidList) {
    this.uuidList = uuidList;
  }
}
