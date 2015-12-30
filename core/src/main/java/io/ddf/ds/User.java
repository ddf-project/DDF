package io.ddf.ds;


import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class User {

  private static final ThreadLocal<User> currentUser = new ThreadLocal<>();

  public static User getCurrentUser() {
    return currentUser.get();
  }

  public static void setCurrentUser(User user) {
    currentUser.set(user);
  }

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

  public DataSourceCredential removeCredential(String sourceUri) {
    return authenticatedSources.remove(sourceUri);
  }

  public boolean hasCredentialFor(String sourceUri) {
    return authenticatedSources.containsKey(sourceUri);
  }

  public Map<String, DataSourceCredential> getAuthenticatedSources() {
    // don't allow the returned map to be modified
    return Collections.unmodifiableMap(authenticatedSources);
  }
}
