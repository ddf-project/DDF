package io.ddf.datasource;

/**
 * author: namma,
 * created on 4/5/2015
 */

import io.ddf.DDF;
import io.ddf.DDFManager;

import java.net.URISyntaxException;

class JDBCDataSourceCredentials implements IDataSourceCredentials {
  String username;
  String password;

  public JDBCDataSourceCredentials(String username, String password) {
      this.username = username;
      this.password = password;
  }
}

public class JDBCDataSourceDescriptor extends DataSourceDescriptor {
  public JDBCDataSourceDescriptor(DataSourceURI uri, JDBCDataSourceCredentials credentials, String dbTable) {
    super(uri, credentials, null, null);
  }

  public JDBCDataSourceDescriptor(String uri, String username, String password, String dbTable) throws URISyntaxException {
    this(new DataSourceURI(uri), new JDBCDataSourceCredentials(username, password), dbTable);
  }
  @Override
  public DDF load(DDFManager manager) {
    return null;
  }
}

