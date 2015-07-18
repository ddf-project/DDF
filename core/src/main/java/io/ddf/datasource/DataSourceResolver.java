package io.ddf.datasource;

import io.ddf.exception.DDFException;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * author: daoduchuan, namma
 *
 */

public class DataSourceResolver {
  public static DataSourceDescriptor resolve(String source,
                        HashMap<String, String> options) throws DDFException, URISyntaxException {
    switch (source) {
      case "S3": {
        return resolveS3(options);
      }
      case "hdfs": {
        return resolveHDFS(options);
      }
      case "jdbc": {
        return resolveJDBC(options);
      }
      case "sql" : {
        return resolveSQL(options);
      }
      default: {
        throw  new DDFException("Error, could not find data-source for " + source);
      }
    }
  }

  private static String getOrDefault(Map<String, String> map, String key, String defaultVal){
    return map.containsKey(key) ? map.get(key) : defaultVal;
  }

  public static S3DataSourceDescriptor resolveS3(HashMap<String, String> options) throws DDFException {
    String uri = options.get("uri");
    String awsKeyID = getOrDefault(options,"awsKeyID", "");
    String awsSecretKey = getOrDefault(options,"awsSecretKey", "");
    String schema = options.get("schema");
    // TODO format null?
    DataFormat format = DataFormat.fromInt(Integer.parseInt(options.get("dataFormat")));
    if (options.get("serde") != null) {
      String serde = options.get("serde");
      return new S3DataSourceDescriptor(uri, awsKeyID, awsSecretKey, schema, serde, format);
    } else {
      String hasHeaderString = getOrDefault(options,"hasheader", "false");
      Boolean hasheader = Boolean.valueOf(hasHeaderString);
      String delim = getOrDefault(options,"delim", ",");
      String quote = getOrDefault(options,"quote", "\"");
      return new S3DataSourceDescriptor(uri, awsKeyID, awsSecretKey, schema, format, hasheader, delim, quote);
    }
  }


  public static HDFSDataSourceDescriptor resolveHDFS(HashMap<String, String> options) throws DDFException, URISyntaxException {
    String uri = options.get("uri");
    String schema = getOrDefault(options,"schema", null);
    String originalSource = getOrDefault(options,"originalSource", "hdfs");
    DataFormat format = DataFormat.fromInt(Integer.parseInt(options.get("dataFormat")));
    if(options.containsKey("serde")) {
      String serde = options.get("serde");
      return new HDFSDataSourceDescriptor(uri, schema, serde, originalSource, format);
    } else {
      String delim = getOrDefault(options,"delim", ",");
      String quote = getOrDefault(options,"quote", "\"");
      Boolean hasHeader = Boolean.parseBoolean(getOrDefault(options,"hasheader", "false"));
      return new HDFSDataSourceDescriptor(uri, schema, format, hasHeader, delim, quote, originalSource);
    }
  }

  public static JDBCDataSourceDescriptor resolveJDBC(HashMap<String, String> options) throws DDFException {
    String uri = options.get("uri");
    String username = options.get("username");
    String password = options.get("password");
    String dbTable = options.get("dbTable");

    try {
      return new JDBCDataSourceDescriptor(uri, username, password, dbTable);
    } catch (URISyntaxException e) {
      throw new DDFException(e);
    }
  }

  public static SQLDataSourceDescriptor resolveSQL(HashMap<String, String> options) {
    String sql = options.get("sqlCmd");
    String namespace = getOrDefault(options,"namespace", null);
    String uriListStr = getOrDefault(options,"uriListStr", null);
    String uuidListStr = getOrDefault(options,"uuidListStr", null);
    String dataSource = getOrDefault(options,"dataSource", null);
    // val ddfList = options("ddfList")
    return new SQLDataSourceDescriptor(sql, dataSource, namespace, uriListStr, uuidListStr);
  }

}
