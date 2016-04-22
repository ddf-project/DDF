package io.ddf.hdfs;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.HDFSDataSourceDescriptor;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;
import io.ddf.util.Utils;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by jing on 2/22/16.
 */
//TODO: Extract FileDDFManager as an abstract class for s3 and hdfs.
public class HDFSDDFManager extends DDFManager {
  // File system connector.
  private FileSystem fs = null;
  // Upper limit for content preview.
  private static final int K_LIMIT = 1000;
  private String fsUri = null;

  public HDFSDDFManager(HDFSDataSourceDescriptor hdfsDataSourceDescriptor, EngineType engineType) throws DDFException {
    this(hdfsDataSourceDescriptor.getDataSourceUri().getUri().toString());
  }

  public HDFSDDFManager(String fsUri) throws DDFException {
    assert !Strings.isNullOrEmpty(fsUri);
    this.fsUri = fsUri;
    try {
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", fsUri);
      this.fs = FileSystem.get(conf);
    } catch (Exception e) {
      throw new DDFException(e);
    }
  }

  /**
   * @brief To check whether the ddf is a directory.
   */
  public Boolean isDir(HDFSDDF hdfsDDF) throws DDFException {
    String path = hdfsDDF.getPath();
    try {
      FileStatus fileStatus = fs.getFileStatus(new Path(path));
      return fileStatus.isDirectory();
    } catch (IOException e) {
      throw new DDFException(e);
    }
  }

  /**
   * @breif To get the dataformat.
   */
  public DataFormat getDataFormat(HDFSDDF hdfsDDF) throws DDFException {
    if (hdfsDDF.getIsDir()) {
      try {
        FileStatus[] files = this.fs.listStatus(new Path(hdfsDDF.getPath()));
        HashSet<DataFormat> dataFormats = new HashSet<>();
        for (FileStatus file : files) {
          if (file.isDirectory()) {
            throw new DDFException("This folder contains subfolder, we currently do not support nested folders");
          }
          String filePath = file.getPath().toString();
          // Check for extension.
          DataFormat dataFormat = Utils.getDataFormatFromPath(filePath);
          if (!dataFormat.equals(DataFormat.UNDEF)) {
            dataFormats.add(dataFormat);
          }
        }
        if (dataFormats.size() > 1) {
          throw new DDFException(String.format("Find more than 1 formats of data under the directory %s: %s", hdfsDDF
              .getPath(), Arrays.toString(dataFormats.toArray())));
        } else if (dataFormats.size() == 1) {
          return dataFormats.iterator().next();
        } else {
          return DataFormat.CSV;
        }
      } catch (IOException e) {
        throw new DDFException(String.format("Can't open directory : %s", hdfsDDF.getPath()));
      }
    } else {
      String filePath = hdfsDDF.getPath();
      // Check for extension.
      DataFormat dataFormat = Utils.getDataFormatFromPath(filePath);
      return dataFormat.equals(DataFormat.UNDEF) ? DataFormat.CSV : dataFormat;
    }
  }

  /**
   * @param path The path.
   * @return The list of file names
   * @brief List all the files (including directories under one path)
   */
  public List<String> listFiles(String path) throws DDFException {
    List<String> ret = new ArrayList<>();
    try {
      FileStatus[] status = fs.listStatus(new Path(path));
      for (int i = 0; i < status.length; i++) {
        ret.add(status[i].getPath().getName().toString());
      }
    } catch (IOException e) {
      throw new DDFException(e);
    }
    return ret;
  }


  /**
   * @param path The path.
   * @brief Create a ddf given path.
   */

  public HDFSDDF newDDF(String path) throws DDFException {
    return this.newDDF(path, null);
  }

  public HDFSDDF newDDF(String path, Map<String, String> options) throws DDFException {
    return this.newDDF(path, null, options);
  }

  public HDFSDDF newDDF(String path, String schema, Map<String, String> options) throws DDFException {
    return new HDFSDDF(this, path, schema, options);
  }


  /**
   * @brief Show the first several rows of the s3ddf.
   */
  public List<String> head(HDFSDDF hdfsDDF, int limit) throws DDFException {
    if (limit > K_LIMIT) {
      limit = K_LIMIT;
    }

    List<String> rows = new ArrayList<String>();

    int pos = 0;
    String s = null;

    if (!hdfsDDF.getIsDir()) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsDDF.getPath()))))) {
        while ((s = br.readLine()) != null && pos < limit) {
          rows.add(s);
          ++pos;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      RemoteIterator<LocatedFileStatus> files = null;
      try {
        files = fs.listFiles(new Path(hdfsDDF.getPath()), false);
        while (files.hasNext() && pos < limit) {
          LocatedFileStatus lfs = files.next();
          try (BufferedReader br = new BufferedReader(
              new InputStreamReader(
                  fs.open(lfs.getPath())))) {
            while ((s = br.readLine()) != null && pos < limit) {
              rows.add(s);
              ++pos;
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } catch (IOException e) {
        throw new DDFException(e);
      }
    }
    return rows;
  }

  @Override
  public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
    return null;
  }

  @Override
  public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
    return null;
  }

  @Override
  public DDF copyFrom(DDF fromDDF) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public DDF createDDF(Map<Object, Object> options) throws DDFException {
    return null;
  }

  @Override
  public void validateCredential(DataSourceCredential credential) throws DDFException {

  }

  @Override
  public String getSourceUri() {
    return this.fsUri;
  }

  @Override
  public String getEngine() {
    return "hdfs";
  }

  public void stop() {
    // close connection
  }
}