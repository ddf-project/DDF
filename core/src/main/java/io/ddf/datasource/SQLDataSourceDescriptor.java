package io.ddf.datasource;
/**
 * author: daoduchuan
 */

import io.ddf.DDF;
import io.ddf.DDFManager;


import java.util.Arrays;
import java.util.List;

public class SQLDataSourceDescriptor extends DataSourceDescriptor {
  private String sqlCmd;
  private String dataSource;
  private String  namespace;
  private List<String> uriList;
  private List<String> uuidList;

  public SQLDataSourceDescriptor(DataSourceURI uri, IDataSourceCredentials credentials,
                                 DataSourceSchema schema, FileFormat format) {
    super(uri, credentials, schema, format);
  }

  public SQLDataSourceDescriptor(DataSourceURI uri, IDataSourceCredentials credentials,
                                 DataSourceSchema schema, FileFormat format, String sqlcmd,
                                 String dataSource, String namespace, List<String> uriList,
                                 List<String> uuidList) {
    super(uri, credentials, schema, format);
    this.sqlCmd = sqlcmd;
    this.dataSource = dataSource;
    this.namespace = namespace;
    this.uriList = uriList;
    this.uuidList = uuidList;
  }

  public SQLDataSourceDescriptor(String sqlCommand) {
    this(sqlCommand, null, null, null, null);
  }

  public SQLDataSourceDescriptor(String sqlCommand, String dataSource, String namespace, String uriListStr,
                                 String uuidListStr) {
    super(null, null, null, null);
    this.sqlCmd = sqlCommand;
    this.dataSource = dataSource;
    this.namespace = namespace;
    if (uriListStr != null ) {
      String[] splitArray = uriListStr.split("\t");
      this.uriList = Arrays.asList(splitArray);
    } else if (uuidListStr != null) {
      String[] splitArray = uuidListStr.split("\t");
      this.uuidList = Arrays.asList(splitArray);
    }
  }

//  public SQLDataSourceDescriptor(String sqlCommand, String dataSource, String namespace,
//      List<String> iuriList, List<String> iuuidList) {
//    this.sqlCmd = sqlCommand;
//    this.dataSource = dataSource;
//    this.namespace = namespace;
//
//  }
  // Getters and Setters.
  public String getSqlCommand() {
    return sqlCmd;
  }

  public void setSqlCommand(String sqlCommand) {
    this.sqlCmd = sqlCommand;
  }

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public List<String> getUriList() {
    return uriList;
  }

  public void setUriList(List<String> uriList) {
    this.uriList = uriList;
  }

  public List<String> getUuidList() {
    return uuidList;
  }

  public void setUuidList(List<String> uuidList) {
    this.uuidList = uuidList;
  }

  @Override
  public DDF load(DDFManager manager) { return null; }


}
