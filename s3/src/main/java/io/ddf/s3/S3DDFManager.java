package io.ddf.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.S3DataSourceCredentials;
import io.ddf.exception.DDFException;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by jing on 12/2/15.
 */

public class S3DDFManager extends DDFManager {
    DataSourceDescriptor mDsd;
    // Amazon client connection.
    AmazonS3 mConn;

    public S3DDFManager(DataSourceDescriptor s3dsd) throws DDFException {
        mDsd = s3dsd;
        S3DataSourceCredentials s3cred = (S3DataSourceCredentials)s3dsd.getDataSourceCredentials();
        AWSCredentials credentials = new BasicAWSCredentials(s3cred.getAwsKeyID(), s3cred.getAwsScretKey());
        mConn = new AmazonS3Client(credentials);
    }

    /**
     * @brief To check whether the s3 file already contains header.
     * @param s3uri The uri of the s3 file (single file).
     * @return True if it has header, otherwise false.
     */
    public Boolean hasHeader(String s3uri) {
        // TODO: Should we do this check in backend?
        return false;
    }

    public List<String> listBuckets(String path) {
        List<Bucket> bucketList = mConn.listBuckets();
        List<String> ret = new ArrayList<String>();
        for (Bucket bucket : bucketList) {
            ret.add(bucket.getName());
        }
        return ret;
    }

    /**
     * @brief List all the files (including directories under one path)
     * @param path The path.
     * @return The list of file names (TODO: should we return more info here.)
     */
    public List<String> listFiles(String bucket, String path) {
        List<String> files = new ArrayList<String>();
        ObjectListing objects = mConn.listObjects(bucket, path);
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
    public DDF newDDF(String path) throws DDFException {
        return super.newDDF(this, null, null, null, path, null);
    }


    public DDF newDDF(String path, String schema) throws DDFException {
        // TODO: schema?
        return super.newDDF(this, null, null, null, path, null);
    }

    @Override
    public DDF transfer(UUID fromEngine, UUID ddfuuid) throws DDFException {
        return null;
    }

    @Override
    public DDF transferByTable(UUID fromEngine, String tableName) throws DDFException {
        return null;
    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        return null;
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
    public String getEngine() {
        return "s3";
    }
}