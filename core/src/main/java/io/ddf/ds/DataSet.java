package io.ddf.ds;


import java.net.URI;
import java.util.UUID;

/**
 * Created by huandao on 11/21/15.
 */
public class DataSet {

  private UUID id;

  private UUID dataSourceId;

  private URI uri;

  public DataSet(UUID id, UUID dataSourceId, URI uri) {
    this.id = id;
    this.dataSourceId = dataSourceId;
    this.uri = uri;
  }

  public URI getUri() {
    return uri;
  }

  public void setUri(URI uri) {
    this.uri = uri;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getDataSourceId() {
    return dataSourceId;
  }

  public void setDataSourceId(UUID dataSourceId) {
    this.dataSourceId = dataSourceId;
  }
}
