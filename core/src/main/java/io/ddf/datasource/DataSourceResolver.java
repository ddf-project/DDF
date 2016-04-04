package io.ddf.datasource;

import io.ddf.exception.DDFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * author: daoduchuan, namma
 *
 */

public class DataSourceResolver {
  private static Logger LOG = LoggerFactory.getLogger(DataSourceResolver.class);

  public static DataSourceDescriptor resolve(String source,
                        Map<String, String> options) throws DDFException, URISyntaxException {
    switch (source.toLowerCase()) {
      case "s3": {
        return resolveS3(options);
      }
      case "hdfs": {
        return resolveHDFS(options);
      }
      case "jdbc":case "sfdc":case "postgres":case "redshift": {
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

  public static S3DataSourceDescriptor resolveS3(Map<String, String> options) throws DDFException {
    if (LOG.isInfoEnabled()) {
      Map<String, String> sourceOptions = new HashMap<>(options);
      sourceOptions.put("awsSecretKey", "<redacted>");
      LOG.info("Loading from S3 with options: {}", sourceOptions);
    }

    String uri = getOrDefault(options, "uri", "");
    String awsKeyID = getOrDefault(options, "awsKeyID", "");
    String awsSecretKey = getOrDefault(options,"awsSecretKey", "");
    String schema = getOrDefault(options, "schema", null);
    DataFormat format = options.containsKey("dataFormat")?
        DataFormat.fromInt(Integer.parseInt(options.get("dataFormat"))) :
        DataFormat.CSV;
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


  public static HDFSDataSourceDescriptor resolveHDFS(Map<String, String> options) throws DDFException, URISyntaxException {
    String uri = options.get("uri");
    String schema = getOrDefault(options, "schema", null);
    String originalSource = getOrDefault(options,"originalSource", "hdfs");
    DataFormat format = options.containsKey("dataFormat")?
        DataFormat.fromInt(Integer.parseInt(options.get("dataFormat"))) :
        DataFormat.CSV;
    LOG.info("Loading from HDFS with options: {}", options);
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

  public static JDBCDataSourceDescriptor resolveJDBC(Map<String, String> options) throws DDFException {
    if (LOG.isInfoEnabled()) {
      Map<String, String> sourceOptions = new HashMap<>(options);
      sourceOptions.put("password", "<redacted>");
      LOG.info("Loading from S3 with options: {}", sourceOptions);
    }

    String uri = options.get("uri");
    String username = options.get("username");
    String password = options.get("password");
    // String dbTable = options.get("dbTable");
    String dbTable = getOrDefault(options, "dbTable", null);
    try {
      return new JDBCDataSourceDescriptor(uri, username, password, dbTable);
    } catch (URISyntaxException e) {
      throw new DDFException(e);
    }
  }

  public static SQLDataSourceDescriptor resolveSQL(Map<String, String> options) {
    String sql = options.get("sqlCmd");
    String namespace = getOrDefault(options,"namespace", null);
    String uriListStr = getOrDefault(options,"uriListStr", null);
    String uuidListStr = getOrDefault(options,"uuidListStr", null);
    String dataSource = getOrDefault(options,"dataSource", null);
    // val ddfList = options("ddfList")
    LOG.info("Loading from SQL with options: {}", options);
    return new SQLDataSourceDescriptor(sql, dataSource, namespace, uriListStr, uuidListStr);
  }

}
