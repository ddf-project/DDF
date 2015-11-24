package io.ddf.ds;


import java.util.UUID;

/**
 */
public class DSUserCredentials {

  protected UUID id;

  protected String userId;

  protected UUID dataSourceId;

  protected String username;

  protected String passWord;

  public DSUserCredentials(UUID id, String userId, UUID dataSourceId, String username, String passWord) {
    this.id = id;
    this.userId = userId;
    this.dataSourceId = dataSourceId;
    this.username = username;
    this.passWord = passWord;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public UUID getDataSourceId() {
    return dataSourceId;
  }

  public void setDataSourceId(UUID dataSourceId) {
    this.dataSourceId = dataSourceId;
  }

  public String getUsername() {
    return username;
  }

  public String getPassWord() {
    return passWord;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }
}
