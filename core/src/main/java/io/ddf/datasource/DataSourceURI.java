package io.ddf.datasource;


import java.net.URI;

/**
 */
public class DataSourceURI {
  protected URI mUri;

  public DataSourceURI(URI uri) {
    this.mUri = uri;
  }

  public DataSourceURI(String uriString) throws java.net.URISyntaxException {
    this.mUri = new URI(uriString);
  }

  public void setUri(URI uri) {
    mUri = uri;
  }

  public URI getUri() {
    return this.mUri;
  }
}
