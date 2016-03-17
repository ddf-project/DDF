package io.ddf.s3;

import com.amazonaws.services.s3.model.S3DataSource;
import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.exception.DDFException;

import org.apache.hadoop.fs.s3.S3Credentials;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDF extends DDF {
    // Whether this ddf has header.
    private Boolean mHasHeader = false;

    // It's a directory or file.
    private Boolean mIsDir;

    // The format of this s3ddf. If it's a folder, we requires that all the files in the folder should have the same
    // format, otherwise the dataformat will be set to the dataformat of the first file under this folder.
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
        initialize();
    }

    public S3DDF(S3DDFManager manager, String path) throws DDFException {
        super(manager, null, null, null, null, null);
        this.getBucketAndPath(path);
        initialize();
    }

    public S3DDF(S3DDFManager manager, String bucket, String key, String schema) throws DDFException {
        super(manager, null, null, null, null, null);
        mBucket = bucket;
        mKey = key;
        mSchemaString = schema;
        initialize();
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

    private void initialize() throws DDFException {
        // Check key and path
        if (Strings.isNullOrEmpty(mBucket)) {
            throw new DDFException("The bucket of s3ddf is null");
        }
        if (Strings.isNullOrEmpty(mKey)) {
            throw new DDFException("The key of s3ddf is null");
        }
        // Check directory or file.
        S3DDFManager s3DDFManager = (S3DDFManager)this.getManager();
        mIsDir = s3DDFManager.isDir(this);
        // Check dataformat.
        mDataFormat = s3DDFManager.getDataFormat(this);
        if (mDataFormat.equals(DataFormat.CSV)) {
            // Check header.
            // TODO (discuss with bigapps guy)
        } else {
            mHasHeader = false;
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
    public S3DDFManager getManager() {
        return (S3DDFManager)super.getManager();
    }

    @Override
    public DDF copy() throws DDFException {
        return null;
    }
}
