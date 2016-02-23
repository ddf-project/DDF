package io.ddf.hdfs;

import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.datasource.DataFormat;
import io.ddf.exception.DDFException;

/**
 * Created by jing on 2/22/16.
 */
public class HDFSDDF extends DDF {
    // Whether this ddf has header.
    private Boolean mHasHeader = false;

    // It's a directory or file.
    private Boolean mIsDir;

    // The format of this s3ddf. If it's a folder, we requires that all the files in the folder should have the same
    // format, otherwise the dataformat will be set to the dataformat of the first file under this folder.
    private DataFormat mDataFormat;

    // Schema String.
    private String mSchemaString;

    // File path.
    private String mPath;

    /**
     * S3DDF is the ddf for s3. It point to a single S3DDFManager, and every S3DDF is a unqiue mapping to a s3 uri.
     * The schema should store the s3 uri as tablename.
     */
    public HDFSDDF(HDFSDDFManager manager, String path, String schema) throws DDFException {
        super(manager, null, null, null, null, null);
        mSchemaString = schema;
        mPath = path;
        initialize();
    }

    public HDFSDDF(HDFSDDFManager manager, String path) throws DDFException {
        super(manager, null, null, null, null, null);
        mPath = path;
        initialize();
    }


    private void initialize() throws DDFException {
        // Check key and path
        if (Strings.isNullOrEmpty(mPath)) {
            throw new DDFException("The path of hdfsddf is null");
        }
        // Check directory or file.
        HDFSDDFManager hdfsDDFManager = (HDFSDDFManager)this.getManager();
        mIsDir = hdfsDDFManager.isDir(this);
        // Check dataformat.
        mDataFormat = hdfsDDFManager.getDataFormat(this);
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

    @Override
    public DDF copy() throws DDFException {
        return null;
    }
}
