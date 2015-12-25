package io.ddf.ds;

import java.util.Map;

public class UsernamePasswordCredential implements DataSourceCredential {
  private final String username;
  private final String password;

  public UsernamePasswordCredential(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public static UsernamePasswordCredential from(Map<Object, Object> options) {
    Object username = options.get("username");
    Object password = options.get("password");
    return new UsernamePasswordCredential(
        username == null ? "" : username.toString(),
        password == null ? "" : password.toString());
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
