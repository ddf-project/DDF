package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;
import io.ddf.spark.ds.DataSource;
import io.ddf.spark.ds.FileDataSource;
import io.ddf.spark.ds.JdbcDataSource;
import io.ddf.spark.ds.S3DataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A DDFManager that delegate work to another DDFManager.
 * This is used to re-use one single SparkDDFManager with multiple DataSource, so that
 * it appears as each data source has one and only one corresponding DDFManager.
 */
public class DelegatingDDFManager extends DDFManager {

  private final DDFManager manager;
  private final String uri;
  private final DataSource dataSource;

  public DelegatingDDFManager(DDFManager manager, String uri) throws DDFException {
    this.uri = uri;
    this.manager = manager;
    if (uri.startsWith("s3:")) {
      dataSource = new S3DataSource(uri, manager);
    } else if (uri.startsWith("hdfs:") || uri.startsWith("file:")) {
      dataSource = new FileDataSource(uri, manager);
    } else if (uri.startsWith("jdbc:")) {
      dataSource = new JdbcDataSource(uri, manager);
    } else {
      throw new DDFException("Unsupported datasource " + uri);
    }
  }

  @Override
  public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
    return manager.transfer(fromEngine, ddfuuid);
  }

  @Override
  public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
    return manager.transferByTable(fromEngine, tableName);
  }

  @Override
  public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    return manager.loadTable(fileURL, fieldSeparator);
  }

  @Override
  public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
    return manager.getOrRestoreDDFUri(ddfURI);
  }

  @Override
  public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
    return manager.getOrRestoreDDF(uuid);
  }

  @Override
  public DDF createDDF(Map<Object, Object> options) throws DDFException {
    // clone the options so that we can add our new field for source uri
    options = new HashMap<>(options);
    options.put("sourceUri", uri);
    options.put("dataSource", dataSource);
    return manager.createDDF(options);
  }

  @Override
  public void validateCredential(DataSourceCredential credential) throws DDFException {
    dataSource.validateCredential(credential);
  }

  @Override
  public String getSourceUri() {
    return uri;
  }

  @Override
  public String getEngine() {
    return null;
  }
}
