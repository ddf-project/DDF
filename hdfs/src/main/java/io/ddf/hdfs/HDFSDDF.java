package io.ddf.hdfs;

import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;

import java.util.Map;

/**
 * Created by jing on 2/22/16.
 */
public class HDFSDDF extends DDF {
    // It's a directory or file.
    private Boolean mIsDir;

    // The format of this s3ddf. If it's a folder, we requires that all the files in the folder should have the same
    // format, otherwise the dataformat will be set to the dataformat of the first file under this folder.
    private DataFormat mDataFormat;

    // Schema String.
    private String mSchemaString;

    // File path.
    private String mPath;

    private Map<String, String> options;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     * The schema should store the s3 uri as tablename.
     */
    public HDFSDDF(HDFSDDFManager manager, String path, String schema, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        mSchemaString = schema;
        mPath = path;
        this.options = options;
        initialize();
    }

    public HDFSDDF(HDFSDDFManager manager, String path, Map<String, String> options) throws DDFException {
        super(manager, null, null, null, null, null);
        mPath = path;
        this.options = options;
        initialize();
    }


    private void initialize() throws DDFException {
        // Check key and path
        if (Strings.isNullOrEmpty(mPath)) {
            throw new DDFException("The path of hdfsddf is null");
        }
        // Check directory or file.
        HDFSDDFManager hdfsDDFManager = this.getManager();
        mIsDir = hdfsDDFManager.isDir(this);
        // Check dataformat.
        if (options != null && options.containsKey("format")) {
            try {
                mDataFormat = DataFormat.valueOf(options.get("format"));
            } catch (IllegalArgumentException e) {
                mDataFormat = hdfsDDFManager.getDataFormat(this);
            }
        } else {
            mDataFormat = hdfsDDFManager.getDataFormat(this);
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

    public String getPath() {
        return mPath;
    }

    public void setPath(String path) {
        this.mPath = path;
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
    public HDFSDDFManager getManager() {
        return (HDFSDDFManager)super.getManager();
    }

    @Override
    public DDF copy() throws DDFException {
        throw new DDFException(new UnsupportedOperationException());
    }
}
