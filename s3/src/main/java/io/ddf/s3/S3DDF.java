package io.ddf.s3;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDF extends DDF {
    // Whether this ddf has header.
    private Boolean mHasHeader;
    // It's a directory or file.
    private Boolean mIsDir;
    // The format of this s3ddf.
    private DataFormat mDataFormat;
    // Schema String.
    private String mSchemaString;
    // Bucket.
    private String mBucket;
    // Path after bucket.
    private String mKey;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     * The schema should store the s3 uri as tablename.
     */


    public S3DDF(S3DDFManager manager, String path, String schema) throws DDFException {
        super(manager, null, null, null, null, null);
        mSchemaString = schema;
        this.getBucketAndPath(path);
        checkStatus();
    }

    public S3DDF(S3DDFManager manager, String path) throws DDFException {
        super(manager, null, null, null, null, null);
        this.getBucketAndPath(path);
        checkStatus();
    }

    public S3DDF(S3DDFManager manager, String bucket, String pathNoBucket, String schema) throws DDFException {
        super(manager, null, null, null, null, null);
        mBucket = bucket;
        mKey = pathNoBucket;
        mSchemaString = schema;
        checkStatus();
    }


    /**
     * @brief Get the bucket and path out of a given uri.
     * @param path
     * @return
     */
    private void getBucketAndPath(String path) {
        int firstSlash = path.indexOf('/');
        mBucket = path.substring(0, firstSlash);
        mKey = path.substring(firstSlash + 1);
    }

    public void checkStatus() {
        // Check directory or file.
        S3DDFManager s3DDFManager = (S3DDFManager)this.getManager();
        mIsDir = s3DDFManager.isDir(this);
        // Check dataformat.
        if (!mIsDir) {
            mDataFormat = s3DDFManager.getDataFormat(this);
            if (mDataFormat.equals(DataFormat.CSV)) {
                // Check header.
                // TODO (discuss with bigapps guy)
            } else {
                mHasHeader = false;
            }
        }
    }

    public Boolean getHasHeader() {
        return mHasHeader;
    }

    public void setHasHeader(Boolean hasHeader) {
        this.mHasHeader = hasHeader;
    }

    public DataFormat getDataFormat() {
        return mDataFormat;
    }

    public void setDataFormat(DataFormat dataFormat) {
        this.mDataFormat = dataFormat;
    }

    public Boolean getIsDir() {
        return mIsDir;
    }

    public void setIsDir(Boolean isDir) {
        this.mIsDir = isDir;
    }

    public String getBucket() {
        return mBucket;
    }

    public void setBucket(String bucket) {
        this.mBucket = bucket;
    }

    public String getKey() {
        return mKey;
    }

    public void setKey(String key) {
        this.mKey = key;
    }

    public String getSchemaString() {
        return mSchemaString;
    }

    public void setSchemaString(String schemaString) {
        this.mSchemaString = schemaString;
    }

    @Override
    public DDF copy() throws DDFException {
        return null;
    }
}
