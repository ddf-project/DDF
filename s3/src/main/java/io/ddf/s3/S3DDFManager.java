package io.ddf.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDFManager extends DDFManager {
  // Descriptor.
  private S3DataSourceCredentials mCredential;

  // Amazon client connection.
  private AmazonS3 mConn;

  // Upper limit for content preview.
  private static final int K_LIMIT = 1000;

  // TODO: Remove Engine Type
  public S3DDFManager(S3DataSourceDescriptor s3dsd, EngineType engineType) throws DDFException {
    this((S3DataSourceCredentials) s3dsd.getDataSourceCredentials(), engineType);
  }

  public S3DDFManager(S3DataSourceCredentials s3Credentials, EngineType engineType) throws DDFException {
    mCredential = s3Credentials;
    AWSCredentials credentials = new BasicAWSCredentials(mCredential.getAwsKeyID(), mCredential.getAwsScretKey());
    mConn = new AmazonS3Client(credentials);
  }

  public S3DDFManager(S3DataSourceDescriptor s3dsd) throws DDFException {
    this((S3DataSourceCredentials) s3dsd.getDataSourceCredentials());
  }

  public S3DDFManager(S3DataSourceCredentials s3Credentials) throws DDFException {
    this(s3Credentials, EngineType.S3);
  }

  /**
   * @brief To check whether the ddf is a directory.
   */
  public Boolean isDir(S3DDF s3DDF) throws DDFException {
    S3Object s3Object = mConn.getObject(s3DDF.getBucket(), s3DDF.getKey());
    List<String> keys = this.listFiles(s3DDF.getBucket(), s3Object.getKey());
    for (String key : keys) {
      if (key.endsWith("/") && !key.equals(s3DDF.getKey())) {
        throw new DDFException("This folder contains subfolder, S3 DDF does not support nested folders");
      }
    }
    return s3Object.getKey().endsWith("/");
  }

  /**
   * @breif To get the dataformat.
   */
  public DataFormat getDataFormat(S3DDF s3DDF) throws DDFException {
    if (s3DDF.getIsDir()) {
      List<String> keys = this.fileKeys(s3DDF);
      if (keys.isEmpty()) {
        throw new DDFException("There is no file under " + s3DDF.getBucket() + "/" + s3DDF.getKey());
      } else {
        HashSet<DataFormat> dataFormats = new HashSet<>();
        for (String key : keys) {
          int dotIndex = key.lastIndexOf('.');
          if (dotIndex != -1) {
            String extension = key.substring(dotIndex + 1);
            try {
              if (extension.equalsIgnoreCase("parquet")) {
                extension = "pqt";
              }
              DataFormat dataFormat = DataFormat.valueOf(extension.toUpperCase());
              dataFormats.add(dataFormat);
            } catch (Exception e) {
              throw new DDFException(String.format("Unsupported dataformat: %s", extension));
            }
          }
        }
        if (dataFormats.size() > 1) {
          throw new DDFException(String.format("Find more than 1 formats of data under the directory %s: " +
              "%s", s3DDF.getKey(), dataFormats.toArray().toString()));
        }
        return dataFormats.iterator().next();
      }
    } else {
      String key = s3DDF.getKey();
      int dotIndex = key.lastIndexOf('.');
      if (dotIndex != -1) {
        return DataFormat.valueOf(key.substring(dotIndex + 1).toUpperCase());
      } else {
        // CSV by default
        return DataFormat.CSV;
      }
    }
  }

  /**
   * @brief List buckets.
   */
  public List<String> listBuckets() {
    List<Bucket> bucketList = mConn.listBuckets();
    List<String> ret = new ArrayList<String>();
    for (Bucket bucket : bucketList) {
      ret.add(bucket.getName());
    }
    return ret;
  }

  /**
   * @param bucket The bucket.
   * @param key    The key.
   * @return The list of file names (TODO: should we return more info here.)
   * @brief List all the files (including directories under one path)
   */
  public List<String> listFiles(String bucket, String key) {
    List<String> files = new ArrayList<String>();
    ObjectListing objects = mConn.listObjects(bucket, key);
    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
      files.add(objectSummary.getKey());
    }
    return files;
  }


  /**
   * @param path The path.
   * @brief Create a ddf given path.
   */
  public S3DDF newDDF(String path, Map<String, String> options) throws DDFException {
    return new S3DDF(this, path, options);
  }

  public S3DDF newDDF(String path, String schema, Map<String, String> options) throws DDFException {
    return new S3DDF(this, path, schema, options);
  }

  public S3DDF newDDF(String bucket, String key, String schema, Map<String, String> options) throws DDFException {
    return new S3DDF(this, bucket, key, schema, options);
  }

  /**
   * @param s3DDF The s3ddf.
   * @brief Return the key of the first non-folder file under this folder.
   */
  private String firstFileKey(S3DDF s3DDF) throws DDFException {
    if (s3DDF.getIsDir()) {
      List<String> ret = this.fileKeys(s3DDF);
      if (ret.isEmpty()) {
        throw new DDFException("There is no file under " + s3DDF.getBucket() + "/" + s3DDF.getKey());
      } else {
        return ret.get(0);
      }
    } else {
      return s3DDF.getKey();
    }
  }

  private List<String> fileKeys(S3DDF s3DDF) throws DDFException {
    List<String> ret = new ArrayList<String>();
    ObjectListing objectListing = mConn.listObjects(new ListObjectsRequest().withBucketName(s3DDF.getBucket())
        .withPrefix(s3DDF.getKey()));
    for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
      if (!summary.getKey().endsWith("/")) {
        ret.add(summary.getKey());
      }
    }
    return ret;
  }

  /**
   * @brief Show the first several rows of the s3ddf.
   */
  public List<String> head(S3DDF s3DDF, int limit) throws DDFException {
    if (limit > K_LIMIT) {
      limit = K_LIMIT;
    }

    String bucket = s3DDF.getBucket();
    List<String> keys = this.fileKeys(s3DDF);
    List<String> rows = new ArrayList<String>();

    for (int i = 0; i < keys.size() && limit > 0; ++i) {
      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(mConn.getObject(bucket, keys.get(i)).getObjectContent()))) {
        String line = null;
        while (limit > 0 && ((line = br.readLine()) != null)) {
          rows.add(line);
          --limit;
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
  public DDF createDDF(Map<Object, Object> options) throws DDFException {
    return null;
  }

  @Override
  public void validateCredential(DataSourceCredential credential) throws DDFException {

  }

  @Override
  public String getSourceUri() {
    return null;
  }

  @Override
  public DDF copyFrom(DDF fromDDF) throws DDFException {
    throw new DDFException(new UnsupportedOperationException());
  }

  @Override
  public String getEngine() {
    return "s3";
  }

  public S3DataSourceCredentials getCredential() {
    return mCredential;
  }

  public void stop() {
    // TODO: Does s3 connection has to be closed?
  }
}