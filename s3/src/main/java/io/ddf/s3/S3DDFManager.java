package io.ddf.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import io.ddf.DDF;
import io.ddf.DDFManager;

import io.ddf.datasource.DataFormat;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.exception.DDFException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by jing on 12/2/15.
 */

public class S3DDFManager extends DDFManager {
    // Descriptor.
    private DataSourceDescriptor mDsd;
    // Amazon client connection.
    private AmazonS3 mConn;
    // Upper limit for content preview.
    final int K_LIMIT = 10000;


    // TODO: Remove Engine Type
    public S3DDFManager(DataSourceDescriptor s3dsd, EngineType engineType) throws DDFException {
        mDsd = s3dsd;
        S3DataSourceCredentials s3cred = (S3DataSourceCredentials)s3dsd.getDataSourceCredentials();
        AWSCredentials credentials = new BasicAWSCredentials(s3cred.getAwsKeyID(), s3cred.getAwsScretKey());
        mConn = new AmazonS3Client(credentials);
    }

    /**
     * @brief To check whether the s3 file already contains header.
     * @param s3DDF The uri of the s3 file (single file).
     * @return True if it has header, otherwise false.
     */
    public Boolean hasHeader(DDF s3DDF) {
        // TODO: Should we do this check in backend?
        return false;
    }

    /**
     * @brief To check whether the ddf is a directory.
     * @param s3DDF
     * @return
     */
    public Boolean isDir(S3DDF s3DDF) {
        S3Object s3Object = mConn.getObject(s3DDF.getBucket(), s3DDF.getKey());
        return s3Object.getKey().endsWith("/");
    }

    /**
     * @breif To get the dataformat.
     * @param s3DDF
     * @return
     */
    public DataFormat getDataFormat(S3DDF s3DDF) {
        String extension = s3DDF.getKey().substring(s3DDF.getKey().lastIndexOf('.') + 1);
        return DataFormat.valueOf(extension.toUpperCase());
    }

    /**
     * @brief List buckets.
     * @return
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
     * @brief List all the files (including directories under one path)
     * @param bucket The bucket.
     * @param key The key.
     * @return The list of file names (TODO: should we return more info here.)
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
     * @brief Create a ddf given path.
     * @param path The path.
     * @return
     */
    public S3DDF newDDF(String path) throws DDFException {
        return new S3DDF(this, path);
    }

    public S3DDF newDDF(String path, String schema) throws DDFException {
        return new S3DDF(this, path, schema);
    }

    public S3DDF newDDF(String bucket, String key, String schema) throws DDFException {
        return new S3DDF(this, bucket, key, schema);
    }

    /**
     * @brief Show the first several rows of the s3ddf.
     * @param s3DDF
     * @param limit
     * @return
     * @throws DDFException
     */
    public List<String> head(S3DDF s3DDF, int limit) throws DDFException {
        if (limit > K_LIMIT) {
            limit = K_LIMIT;
        }

        String bucket = s3DDF.getBucket();
        String key = s3DDF.getKey();

        if (s3DDF.getIsDir()) {
            // Get the first object and show it's result.
            ObjectListing objectListing = mConn.listObjects(new ListObjectsRequest().withBucketName(bucket)
                .withPrefix(key));
            for (S3ObjectSummary summary: objectListing.getObjectSummaries()) {
                if (!summary.getKey().endsWith("/")) {
                    key = summary.getKey();
                    break;
                }
            }
        }

        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(mConn.getObject(bucket, key).getObjectContent()))) {
        String line = null;
        List<String> rows = new ArrayList<String>();
            while (limit > 0 &&  ((line = br.readLine()) != null)) {
                rows.add(line);
                --limit;
            }
            br.close();
            return rows;
        } catch (IOException e) {
            throw new DDFException(e);
        }

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
    public String getEngine() {
        return "s3";
    }

    public void stop() {
        // TODO: Does s3 connection has to be closed?
    }
}