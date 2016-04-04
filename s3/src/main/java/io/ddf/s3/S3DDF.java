package io.ddf.s3;

import com.amazonaws.services.s3.model.S3DataSource;
import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.datasource.S3DataSourceDescriptor;
import io.ddf.exception.DDFException;

import org.apache.hadoop.fs.s3.S3Credentials;

import java.util.Map;

/**
 * Created by jing on 12/2/15.
 */
public class S3DDF extends DDF {
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

    // Options, including:
    // key : possible values
    // header : true / false
    // format: csv / parquet / etc.
    // delimiter : , \x001
    // quote : "
    // escape : \
    // mode : used for spark
    // charSet:
    // inferSchema:
    // comment:
    // nullvalue:
    // dateformat:
    // flatten : true / false
    private Map<String, String > options;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     */
    public S3DDF(S3DDFManager manager, String path, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        this.getBucketAndPath(path);
        this.options = options;
        initialize();
    }

    public S3DDF(S3DDFManager manager, String path, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        mSchemaString = schema;
        this.getBucketAndPath(path);
        this.options = options;
        initialize();
    }


    public S3DDF(S3DDFManager manager, String bucket, String key, String schema, Map<String, String> options)
        throws DDFException {
        super(manager, null, null, null, null, null);
        mBucket = bucket;
        mKey = key;
        mSchemaString = schema;
        this.options = options;
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

        mLog.info(String.format("init s3 ddf: %s %s", mBucket, mKey));
        // Check directory or file.
        S3DDFManager s3DDFManager = this.getManager();
        // Check dataformat.
        if (options != null && options.containsKey("format")) {
            try {
                mDataFormat = DataFormat.valueOf(options.get("format"));
            } catch (IllegalArgumentException e) {
                mIsDir = s3DDFManager.isDir(this);
                mDataFormat = s3DDFManager.getDataFormat(this);
            }
        } else {
            mIsDir = s3DDFManager.isDir(this);
            mDataFormat = s3DDFManager.getDataFormat(this);
        }
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

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    @Override
    public S3DDFManager getManager() {
        return (S3DDFManager)super.getManager();
    }

    @Override
    public DDF copy() throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }
}
