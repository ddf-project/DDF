package io.ddf.datasource;


/**
 */
abstract public class DataSourceURI {
  private String mUri;

  public DataSourceURI(String uri) {
    this.mUri = uri;
  }

  public void setUri(String uri) {
    this.mUri = uri;
  }

  public String getUri() {
    return this.mUri;
  }
}
