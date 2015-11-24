package io.ddf.ds;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 */
public class User {

  private String id;

  private String firstName;

  private String lastName;

  private String email;

  private List<DSUserCredentials> dsUserCredentialsList = new ArrayList<DSUserCredentials>();

  public User(String id, String firstName, String lastName, String email, List<DSUserCredentials> dsUserCredentials) {
    this.dsUserCredentialsList = dsUserCredentials;
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
  }

  public User(String id, String firstName, String lastName, String email) {
    this(id, firstName, lastName, email, new ArrayList<DSUserCredentials>());
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public void addDsUserCredentials(DSUserCredentials dsUserCredential) {
    this.dsUserCredentialsList.add(dsUserCredential);
  }

  public List<DSUserCredentials> getDsUserCredentialsList() {
    return new ArrayList<DSUserCredentials>(dsUserCredentialsList);
  }

  public void removeDsUserCredentials(UUID id) {
    for(DSUserCredentials cred: dsUserCredentialsList) {
      if(cred.getId() == id) {
        dsUserCredentialsList.remove(cred);
      }
    }
  }
}
