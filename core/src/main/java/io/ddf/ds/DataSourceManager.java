package io.ddf.ds;


import io.ddf.DDF;

/**
 */
public abstract class DataSourceManager {

  public abstract DDF load(User user, DataSet dataset);

  public abstract DDF load(User user, DataSet dataset, DataSource dataSource, DSUserCredentials credentials);

}
