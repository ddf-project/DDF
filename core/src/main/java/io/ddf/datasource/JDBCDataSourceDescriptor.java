package io.ddf.datasource;

import java.net.URISyntaxException;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.DataSourceURI;

public class JDBCDataSourceDescriptor extends DataSourceDescriptor {
  private JDBCDataSourceCredentials credentials;
  private String dbTable;

  public static class JDBCDataSourceCredentials implements IDataSourceCredentials {
    private String username;
    private String password;

    public JDBCDataSourceCredentials(String username, String password){
      this.username = username;
      this.password = password;
    }

    public String getUserName() {
      return username;
    }

    public String getPassword() {
      return password;
    }
  }

  public JDBCDataSourceDescriptor(DataSourceURI uri, JDBCDataSourceCredentials credentials, String dbTable) {
    super(uri, credentials, null, null);
    this.credentials = credentials;
    this.dbTable = dbTable;
  }

  public JDBCDataSourceCredentials getCredentials() {
    return credentials;
  }

  public void setCredentials(JDBCDataSourceCredentials credentials) {
    this.credentials = credentials;
  }

  public String getDbTable() {
    return dbTable;
  }

  public void setDbTable(String dbTable) {
    this.dbTable = dbTable;
  }

  public JDBCDataSourceDescriptor(String uri, String username, String password, String dbTable) throws URISyntaxException {
    this(new DataSourceURI(uri), new JDBCDataSourceCredentials(username, password), dbTable);
  }
  @Override
  public DDF load(DDFManager manager) {
    return null;
  }
}


