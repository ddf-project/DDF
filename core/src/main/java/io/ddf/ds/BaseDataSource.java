package io.ddf.ds;


import io.ddf.DDFManager;

public abstract class BaseDataSource implements DataSource {

  private final String uri;
  private final DDFManager ddfManager;

  public BaseDataSource(String uri, DDFManager ddfManager) {
    this.uri = uri;
    this.ddfManager = ddfManager;
  }

  public String getUri() {
    return uri;
  }

  public DDFManager getDdfManager() {
    return ddfManager;
  }
}
