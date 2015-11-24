package io.ddf.ds;


import java.net.URI;
import java.util.UUID;

/**
 */
public class SQLDataSource extends DataSource {

  private String namespace;

  public SQLDataSource(UUID id, URI uri, String namespace) {
    super(id, uri);
    this.namespace = namespace;
  }
}
