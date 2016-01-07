package io.ddf.ds;


import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represent a DDF user.
 * Each user can keep a list of its own credentials to use on different source.
 */
public class User {

  private static final ThreadLocal<User> currentUser = new ThreadLocal<>();

  /**
   * Get the current user in the system.
   * The current user is keep on a per-thread basis.
   *
   * @return current user
   */
  public static User getCurrentUser() {
    return currentUser.get();
  }

  /**
   * Set the current user in the system.
   * The current user is keep on a per-thread basis.
   *
   * @param user current user
   */
  public static void setCurrentUser(User user) {
    currentUser.set(user);
  }

  private final String id;

  private final String username;

  private final Map<String, DataSourceCredential> authenticatedSources = new ConcurrentHashMap<>();

  /**
   * Create a new user with given username and a random id.
   * @param username the username
   */
  public User(String username) {
    this(UUID.randomUUID().toString(), username);
  }

  /**
   * Create a new user with given id and username.
   * @param id the user id
   * @param username the username
   */
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
  public String toString() {
    return "User{" +
        "id='" + id + '\'' +
        ", username='" + username + '\'' +
        '}';
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

  /**
   * Add a credential for a source.
   * A user can only have one credential for a source,
   * if credential for given source already exists then it will be replaced with the new credential.
   *
   * @param sourceUri URI of the source
   * @param credential credential for given source
   * @return old credential for given source if exists
   */
  public DataSourceCredential addCredential(String sourceUri, DataSourceCredential credential) {
    return authenticatedSources.put(sourceUri, credential);
  }

  /**
   * Get the credential for given source.
   *
   * @param sourceUri URI of the source
   * @return credential for given source, null if not exists
   */
  public DataSourceCredential getCredential(String sourceUri) {
    return authenticatedSources.get(sourceUri);
  }

  /**
   * Remove the credential for given source.
   *
   * @param sourceUri URI of the source
   * @return current credential for given source, null if not exists
   */
  public DataSourceCredential removeCredential(String sourceUri) {
    return authenticatedSources.remove(sourceUri);
  }

  /**
   * Get all sources that this user has credentials.
   *
   * @return mapping from source to credential
   */
  public Map<String, DataSourceCredential> getAuthenticatedSources() {
    // don't allow the returned map to be modified
    return Collections.unmodifiableMap(authenticatedSources);
  }
}
