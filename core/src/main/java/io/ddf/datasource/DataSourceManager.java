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
public abstract class DataSourceManager {

  public DDF load(DataSourceDescriptor dataSourceDescriptor,
                  DDFManager manager) throws DDFException {
    return this.load(dataSourceDescriptor, manager, true);
  }
  public DDF load(DataSourceDescriptor dataSourceDescriptor,
                  DDFManager manager,
                  Boolean persist) throws DDFException {
    // TODO
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

  /**
  public DDF load(, manager: DDFManager, persist: Boolean = true): DDF = {
    val ddf = source match {
      case s3: S3DataSourceDescriptor => loadFromS3(s3, manager)
      case hdfs: HDFSDataSourceDescriptor => loadFromHDFS(hdfs, manager)
      case jdbc: JDBCDataSourceDescriptor => loadFromJDBC(jdbc, manager)
      case sql: SQLDataSourceDescriptor => loadFromSQL(sql, manager)
      case _ => throw new DDFException(s"Cannot find datasource ${source.getClass}")
    }
    ddf.getMetaDataHandler.setDataSourceDescriptor(source)
    val lineage = new DataSourceLineageNode(source = source)
    ddf.getMetaDataHandler.setLineage(lineage)
    if(persist) {
      ddf.getLineageHandler().persistHead()
      ddf.getPersistenceHandler.persistMetaData()
    }
    ddf
  }*/

  private DDF loadSpecialFormat(DataFormat format,
                                URI fileURI,
                                DDFManager manager) {
    return this.loadSpecialFormat(format,
                                  fileURI,
                                  manager,
                                  false);
  }

  // Override
  private DDF loadSpecialFormat(DataFormat format,
                                URI fileURI,
                                DDFManager manager,
                                Boolean flatten) {
    return null;
  }


  private DDF loadTextFile(DataSourceDescriptor dataSource,
                           DDFManager manager) {
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
    // override;
    return null;
  }


  private DDF loadFromS3(S3DataSourceDescriptor dataSource, DDFManager manager) {
    return this.loadExternalFile(dataSource,
            dataSource.getFileFormat().getFormat(),
            dataSource.getDataSourceUri().getUri(),
            manager);
  }

  private DDF loadFromHDFS(HDFSDataSourceDescriptor dataSource, DDFManager manager) {
    return this.loadExternalFile(dataSource,
                                 dataSource.getFileFormat().getFormat(),
                                 dataSource.getDataSourceUri().getUri(),
                                 manager);
  }

  // TODO:override.
  private DDF loadFromJDBC(JDBCDataSourceDescriptor dataSource, DDFManager manager) {
    return null;
  }

  private DDF loadFromSQL(SQLDataSourceDescriptor dataSource, DDFManager manager) throws DDFException {
    if (dataSource.getNamespace() != null) {
      // manager.sql2ddf(dataSource.sqlCmd, null , null, null, dataSource.namespace)
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, dataSource.getNamespace());
    } else if (dataSource.getUriList() != null) {
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, dataSource.getUriList());
    } else if (dataSource.getUuidList() != null) {
      List<UUID> uuidListUUID = new ArrayList<UUID>();
      for (String uuidStr : dataSource.getUuidList()) {
        uuidListUUID.add(UUID.fromString(uuidStr));
      }
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource, (UUID[])uuidListUUID.toArray());
    } else {
      return manager.sql2ddf(dataSource.getSqlCommand(), dataSource);
    }
  }


  private DDF loadExternalFile(DataSourceDescriptor dataSource, DataFormat dataFormat, URI fileURI, DDFManager manager) {

    DDF ddf;
    if (dataFormat.equals(DataFormat.JSON)) {
        ddf = loadSpecialFormat(dataFormat, fileURI, manager);
    } else {
        ddf = loadTextFile(dataSource, manager);
    }
    // TODO override
    // ddf.getMetaDataHandler().setDataSourceDescriptor(dataSource)
    return ddf;
  }

}
