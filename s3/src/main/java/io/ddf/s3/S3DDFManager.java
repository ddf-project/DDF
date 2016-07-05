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
import io.ddf.util.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
    try {
      mConn.getS3AccountOwner();
    } catch (AmazonS3Exception e) {
      throw new DDFException("Credential invalid for s3");
    }
  }

  public S3DDFManager(S3DataSourceDescriptor s3dsd) throws DDFException {
    this((S3DataSourceCredentials) s3dsd.getDataSourceCredentials());
  }

  public S3DDFManager(S3DataSourceCredentials s3Credentials) throws DDFException {
    this(s3Credentials, EngineType.S3);
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
  public List<String> listFiles(String bucket, String key) throws DDFException {
    List<String> files = new ArrayList<String>();
    ListObjectsRequest listObjectRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(key)
        .withDelimiter("/");
    ObjectListing objects;
    do {
      objects = mConn.listObjects(listObjectRequest);
      files.addAll(objects.getCommonPrefixes());
      objects.setMarker(objects.getNextMarker());
    } while (objects.isTruncated());
    Stream<S3ObjectSummary> s3objects = objects.getObjectSummaries().parallelStream();
    files.addAll(s3objects.map(s3ObjectSummary -> s3ObjectSummary.getKey()).collect(Collectors.toList()));
    if (files.size() == 1 && files.get(0).equals(key + "/")) {
      return listFiles(bucket, key + "/");
    }
    if (files.size() == 0) {
      throw new DDFException(new FileNotFoundException("File does not exist"));
    }
    return files;
  }

  public List<String> listFiles(String path) throws DDFException {
    List<String> bucketAndPath = getBucketAndKey(path);
    return this.listFiles(bucketAndPath.get(0), bucketAndPath.get(1));
  }


  /**
   * @param path The path.
   * @brief Create a ddf given path.
   */
  // TODO: switch to builder pattern
  @Deprecated
  public S3DDF newDDF(String path) throws DDFException {
    return this.newDDF(path, null);
  }

  @Deprecated
  public S3DDF newDDF(String path, Map<String, String> options) throws DDFException {
    return this.newDDF(path, null, options);
  }

  @Deprecated
  public S3DDF newDDF(String path, String schema, Map<String, String> options) throws DDFException {
    return new S3DDF(this, path, schema, options);
  }

  public S3DDF newDDF(String bucket, String key, String schema, Map<String, String> options) throws DDFException {
    return new S3DDF(this, bucket, key, schema, options);
  }

  public S3DDF newDDF(List<String> paths, String schema, Map<String, String> options) throws DDFException {
    return new S3DDF(this, paths, schema, options);
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
    return "s3a://" + mCredential.getAwsKeyID() + ":" + mCredential.getAwsScretKey();

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

  /**
   * @brief Get the bucket and path out of a given uri.
   * @param path
   * @return
   */
  public static List<String> getBucketAndKey(String path) throws DDFException {
    int firstSlash = path.indexOf('/');
    if (firstSlash == -1 || firstSlash == path.length()) {
      throw new DDFException(String.format("The path %s is not a valid s3 path", path));
    }
    return Arrays.asList(new String[]{path.substring(0, firstSlash), path.substring(firstSlash + 1)});
  }
}