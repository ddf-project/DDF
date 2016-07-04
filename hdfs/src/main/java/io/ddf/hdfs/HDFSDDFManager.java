package io.ddf.hdfs;

import com.google.common.collect.ImmutableList;
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
import org.apache.log4j.Logger;
import org.apache.spark.deploy.SparkHadoopUtil;

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
      Configuration conf;
      if (fsUri.equals("hdfs:;")) {
        conf = SparkHadoopUtil.get().conf();
      } else {
        conf = new Configuration();
        conf.set("fs.defaultFS", fsUri);
      }
      this.fs = FileSystem.get(conf);
    } catch (Exception e) {
      throw new DDFException(e);
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

  @Deprecated
  public HDFSDDF newDDF(String path) throws DDFException {
    return this.newDDF(path, null);
  }

  @Deprecated
  public HDFSDDF newDDF(String path, Map<String, String> options) throws DDFException {
    return this.newDDF(path, null, options);
  }

  @Deprecated
  public HDFSDDF newDDF(String path, String schema, Map<String, String> options) throws DDFException {
    return new HDFSDDF(this, path, schema, options);
  }

  public HDFSDDF newDDF(String[] paths, String schema, Map<String, String> options) throws DDFException {
    return new HDFSDDF(this, paths, schema, options);
  }

  public HDFSDDF newDDF(List<String> paths, String schema, Map<String, String> options) throws DDFException {
    return new HDFSDDF(this, (String[])paths.toArray(), schema, options);
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
    return fs.getUri().toString();
  }

  @Override
  public String getEngine() {
    return "hdfs";
  }

  public void stop() {
    // close connection
  }
}
