package io.ddf.datasource;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.DataSourceURI;

/**
 * Created by freeman on 7/16/15.
 */
public class JDBCDataSourceDescriptor extends DataSourceDescriptor {

  private String mDBTable;

  @Override public DDF load(DDFManager manager) {
    return null;
  }

  public static class JDBCDataSourceCredentials implements IDataSourceCredentials {
    private String mUserName;
    private String mPassword;

    public void JDBCDataSourceCredentials(String username, String password){
      mUserName = username;
      mPassword = password;
    }

    public String getUserName() {
      return mUserName;
    }

    public String getPassword() {
      return mPassword;
    }
  }

  public JDBCDataSourceDescriptor(DataSourceURI uri, JDBCDataSourceCredentials credentials, String dbTable) {
    super(uri, credentials, null, null);
    mDBTable = dbTable;
  }

}
