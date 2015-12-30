package io.ddf.ds;

import java.util.Map;

public class UsernamePasswordCredential implements DataSourceCredential {

  private final String username;
  private final String password;

  public UsernamePasswordCredential(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public UsernamePasswordCredential(Map<Object, Object> options) {
    Object username = options.get("username");
    Object password = options.get("password");
    this.username = username == null? "" : username.toString();
    this.password  = password == null? "" : password.toString();
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String toString() {
    return "UsernamePasswordCredential{" +
        "username='" + username + '\'' +
        '}';
  }
}
