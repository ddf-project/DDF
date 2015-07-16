package io.ddf.datasource;


import java.net.URI;

/**
 */
public class DataSourceURI {
  private URI mUri;

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

  public String toString() { return this.mUri.toString(); }
}
