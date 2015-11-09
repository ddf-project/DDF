package io.ddf.datasource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import io.ddf.DDFManager;
import io.ddf.DDF;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;

/**
 * author: daoduchuan, namma
 */
public abstract class DataSourceManager {

    protected DDFManager mDDFManager;

    public DataSourceManager(DDFManager manager) {
        this.mDDFManager = manager;
    }

  public  DDF load(DataSourceDescriptor dataSourceDescriptor) throws DDFException {
    return load(dataSourceDescriptor, true);
  }

  // To override
  public  DDF load(DataSourceDescriptor dataSourceDescriptor,
                  Boolean persist) throws DDFException {
    Class sourceClass = dataSourceDescriptor.getClass();
    DDF ddf = null;
    if (sourceClass.equals(S3DataSourceDescriptor.class)) {
      ddf = loadFromS3((S3DataSourceDescriptor)dataSourceDescriptor);
    } else if (sourceClass.equals(HDFSDataSourceDescriptor.class)){
      ddf = loadFromHDFS((HDFSDataSourceDescriptor)dataSourceDescriptor);
    } else if (sourceClass.equals(JDBCDataSourceDescriptor.class)) {
      ddf = loadFromJDBC((JDBCDataSourceDescriptor)dataSourceDescriptor);
    } else if (sourceClass.equals(SQLDataSourceDescriptor.class)) {
      ddf = loadFromSQL((SQLDataSourceDescriptor) dataSourceDescriptor);
    } else {
      throw new DDFException("Cannot find datasource " + sourceClass);
    }
    return ddf;
  }

  public DDF loadSpecialFormat(DataFormat format,
                                URI fileURI) throws DDFException {
    return loadSpecialFormat(format, fileURI, false);
  }

  public DDF loadFromS3(S3DataSourceDescriptor dataSource) throws DDFException {
    return loadExternalFile(dataSource, dataSource.getFileFormat().getFormat(), dataSource.getDataSourceUri().getUri());
  }

  public DDF loadFromHDFS(HDFSDataSourceDescriptor dataSource) throws DDFException {
    return loadExternalFile(dataSource,
            dataSource.getFileFormat().getFormat(),
            dataSource.getDataSourceUri().getUri());
  }


  public DDF loadFromSQL(SQLDataSourceDescriptor dataSource) throws DDFException {
      return this.mDDFManager.sql2ddf(dataSource.getSqlCommand(), dataSource);
  }


  public DDF loadExternalFile(DataSourceDescriptor dataSource, DataFormat dataFormat, URI fileURI) throws DDFException {

    DDF ddf;
    if (dataFormat.equals(DataFormat.JSON)) {
        ddf = loadSpecialFormat(dataFormat, fileURI);
    } else {
        ddf = loadTextFile(dataSource);
    }
    // To override
    // ddf.getMetaDataHandler().setDataSourceDescriptor(dataSource)
    return ddf;
  }

    public abstract DDF loadSpecialFormat(DataFormat format,
                                          URI fileURI,
                                          Boolean flatten) throws DDFException;

    public abstract DDF loadFromJDBC(JDBCDataSourceDescriptor dataSource) throws DDFException;

    public abstract DDF loadTextFile(DataSourceDescriptor dataSource) throws DDFException;


    public void export2csv(DDF ddf, String fileURL, String fieldSeparator, Boolean hasHead) throws DDFException {
        if (!fieldSeparator.equalsIgnoreCase("\t")) {
            throw new DDFException("only '\t' is supported as separator.");
        }
        SqlResult result = ddf.sql("select * from @this", "error");
        this.export2csv(result, fileURL, fieldSeparator, hasHead);
    }

    public void export2csv(SqlResult result, String fileURL, String fieldSeparator, Boolean hasHead)
        throws DDFException {
        if (!fieldSeparator.equalsIgnoreCase("\t")) {
            throw new DDFException("only '\t' is supported as separator.");
        }
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileURL)));
            if (hasHead) {
                bw.write(result.toString());
            } else {
                for (String row : result.getRows()) {
                    bw.write(row + "\n");
                }
            }
            bw.close();
        } catch (IOException e) {
            throw new DDFException(String.format("Error when try to export the result to %s", fileURL));
        }
    }
}
