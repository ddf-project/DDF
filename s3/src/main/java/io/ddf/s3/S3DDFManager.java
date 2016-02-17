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
    private S3DataSourceCredentials mCredential;

    // Amazon client connection.
    private AmazonS3 mConn;

    // Upper limit for content preview.
    private static final int K_LIMIT = 1000;

    // TODO: Remove Engine Type
    public S3DDFManager(S3DataSourceDescriptor s3dsd, EngineType engineType) throws DDFException {
        this((S3DataSourceCredentials)s3dsd.getDataSourceCredentials(), engineType);
    }

    public S3DDFManager(S3DataSourceCredentials s3Credentials, EngineType engineType) throws DDFException {
        mCredential = s3Credentials;
        AWSCredentials credentials = new BasicAWSCredentials(mCredential.getAwsKeyID(), mCredential.getAwsScretKey());
        mConn = new AmazonS3Client(credentials);
    }

    public S3DDFManager(S3DataSourceDescriptor s3dsd) throws DDFException {
        this((S3DataSourceCredentials)s3dsd.getDataSourceCredentials());
    }

    public S3DDFManager(S3DataSourceCredentials s3Credentials) throws DDFException {
        this(s3Credentials, EngineType.S3);
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
     * @param s3DDF
     * @return
     */
    public DataFormat getDataFormat(S3DDF s3DDF) throws DDFException {
        String key = this.firstFileKey(s3DDF);
        String extension = key.substring(key.lastIndexOf('.') + 1);
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
     * @brief Return the key of the first non-folder file under this folder.
     * @param s3DDF The s3ddf.
     * @return
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
        for (S3ObjectSummary summary: objectListing.getObjectSummaries()) {
            if (!summary.getKey().endsWith("/")) {
                ret.add(summary.getKey());
            }
        }
        return ret;
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
        List<String> keys = this.fileKeys(s3DDF);
        List<String> rows = new ArrayList<String>();

        for (int i = 0; i < keys.size() && limit > 0; ++i) {
            try (BufferedReader br = new BufferedReader(
                new InputStreamReader(mConn.getObject(bucket, keys.get(i)).getObjectContent()))) {
                String line = null;
                while (limit > 0 &&  ((line = br.readLine()) != null)) {
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