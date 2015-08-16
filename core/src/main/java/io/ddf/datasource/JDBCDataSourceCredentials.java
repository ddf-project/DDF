package io.ddf.datasource;

/**
 * Created by jing on 7/16/15.
 */
public class JDBCDataSourceCredentials implements IDataSourceCredentials {
    private String username;
    private String password;

    public JDBCDataSourceCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
