package io.ddf.ds;


import com.google.common.base.Preconditions;
import io.ddf.exception.UnauthenticatedDataSourceException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class User {

  private final String id;

  private final String username;

  private final Map<String, DataSourceCredential> authenticatedSources = new ConcurrentHashMap<>();

  public User(String id, String username) {
    Preconditions.checkArgument(id != null, "id cannot be null");
    Preconditions.checkArgument(username != null, "username cannot be null");

    this.id = id;
    this.username = username;
  }

  public String getId() {
    return id;
  }

  public String getUsername() {
    return username;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    User user = (User) o;
    return Objects.equals(id, user.id) &&
        Objects.equals(username, user.username);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, username);
  }

  public DataSourceCredential addCredential(String sourceUri, DataSourceCredential credential) {
    return authenticatedSources.put(sourceUri, credential);
  }

  public DataSourceCredential getCredential(String sourceUri) {
    return authenticatedSources.get(sourceUri);
  }

  public DataSourceCredential removeCredential(DataSource source) {
    return authenticatedSources.remove(source);
  }

  public boolean hasCredentialFor(String sourceUri) {
    return authenticatedSources.containsKey(sourceUri);
  }
}
