package io.ddf.datasource;

import java.net.URI;

import io.ddf.DDFManager;
import io.ddf.DDF;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.ddf.content.Schema;
import io.ddf.exception.DDFException;

/**
 * author: daoduchuan, namma
 */
public  class DataSourceManager {

  public  DDF load(DataSourceDescriptor dataSourceDescriptor,
                  DDFManager manager) throws DDFException {
    return load(dataSourceDescriptor, manager, true);
  }

  // To override
  public  DDF load(DataSourceDescriptor dataSourceDescriptor,
                  DDFManager manager,
                  Boolean persist) throws DDFException {
    Class sourceClass = dataSourceDescriptor.getClass();
    DDF ddf = null;
    if (sourceClass.equals(S3DataSourceDescriptor.class)) {
      ddf = loadFromS3((S3DataSourceDescriptor)dataSourceDescriptor, manager);
    } else if (sourceClass.equals(HDFSDataSourceDescriptor.class)){
      ddf = loadFromHDFS((HDFSDataSourceDescriptor)dataSourceDescriptor, manager);
    } else if (sourceClass.equals(JDBCDataSourceDescriptor.class)) {
      ddf = loadFromJDBC((JDBCDataSourceDescriptor)dataSourceDescriptor, manager);
    } else if (sourceClass.equals(SQLDataSourceDescriptor.class)) {
      ddf = loadFromSQL((SQLDataSourceDescriptor) dataSourceDescriptor, manager);
    } else {
      throw new DDFException("Cannot find datasource " + sourceClass);
    }
    return ddf;
  }


  public DDF loadSpecialFormat(DataFormat format,
                                URI fileURI,
                                DDFManager manager) {
    return loadSpecialFormat(format,
            fileURI,
            manager,
            false);
  }

  // To override
  public DDF loadSpecialFormat(DataFormat format,
                                URI fileURI,
                                DDFManager manager,
                                Boolean flatten) {
    return null;
  }


  public DDF loadTextFile(DataSourceDescriptor dataSource,
                           DDFManager manager) {
      // TODO: discuss about extract a function to generate the sql command.
  /**
    String hiveTableName = UUID.randomUUID().toString().replace("-", "_");
    StringBuilder stringBuilder = new StringBuilder();
    List<Schema.Column> columnList = dataSource.getDataSourceSchema().getColumns();
    for (int i = 0; i < columnList.size(); ++i) {
      if (i == 0) {
        stringBuilder.append(columnList.get(i).getName() + " " + columnList.get(i).getType());
      } else {
        stringBuilder.append(", " + columnList.get(i).getName() + " " + columnList.get(i).getType());
      }
    }
    String schemaStr = stringBuilder.toString();

    TextFileFormat textFileFormat = (TextFileFormat)(dataSource.getFileFormat());
    String quote = textFileFormat.getQuote();
    String delimiter = textFileFormat.getDelimiter();

    String serdesString = "ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde' " +
            "WITH serdeproperties ('separatorChar' = '" +  delimiter + "', 'quoteChar' = '" + quote + "')";

    URI uri = dataSource.getDataSourceUri().getUri();
    String sqlCmd = "create external table " + hiveTableName + " (" + schemaStr  + ") "
            + serdesString + " STORED AS TEXTFILE LOCATION '"+ uri.toString() + "'";
   **/
    // override;
    return null;
  }


  public DDF loadFromS3(S3DataSourceDescriptor dataSource, DDFManager manager) {
    return loadExternalFile(dataSource,
            dataSource.getFileFormat().getFormat(),
            dataSource.getDataSourceUri().getUri(),
            manager);
  }

  public DDF loadFromHDFS(HDFSDataSourceDescriptor dataSource, DDFManager manager) {
    return loadExternalFile(dataSource,
            dataSource.getFileFormat().getFormat(),
            dataSource.getDataSourceUri().getUri(),
            manager);
  }

  // To override.
  public DDF loadFromJDBC(JDBCDataSourceDescriptor dataSource, DDFManager manager) {
    return null;
  }

  public DDF loadFromSQL(SQLDataSourceDescriptor dataSource, DDFManager manager) throws DDFException {
    if (dataSource.getUriList() != null) {
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, dataSource.getUriList());
    } else if (dataSource.getUuidList() != null) {
      List<UUID> uuidListUUID = new ArrayList<UUID>();
      for (String uuidStr : dataSource.getUuidList()) {
        uuidListUUID.add(UUID.fromString(uuidStr));
      }
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, (UUID[])uuidListUUID.toArray());
    } else if (dataSource.getNamespace() != null) {
        // manager.sql2ddf(dataSource.sqlCmd, null , null, null, dataSource.namespace)
        return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, dataSource.getNamespace());
    } else {
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource);
    }
  }


  public DDF loadExternalFile(DataSourceDescriptor dataSource, DataFormat dataFormat, URI fileURI, DDFManager manager) {

    DDF ddf;
    if (dataFormat.equals(DataFormat.JSON)) {
        ddf = loadSpecialFormat(dataFormat, fileURI, manager);
    } else {
        ddf = loadTextFile(dataSource, manager);
    }
    // To override
    // ddf.getMetaDataHandler().setDataSourceDescriptor(dataSource)
    return ddf;
  }

}
