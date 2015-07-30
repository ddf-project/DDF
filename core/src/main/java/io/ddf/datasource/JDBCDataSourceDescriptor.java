package io.ddf.datasource;

import java.net.URISyntaxException;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.DataSourceURI;

public class JDBCDataSourceDescriptor extends SQLDataSourceDescriptor {
  private JDBCDataSourceCredentials credentials;
  private String dbTable;

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


