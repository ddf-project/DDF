package io.ddf2.datasource;

import io.ddf2.datasource.filesystem.IFileFormat;
import io.ddf2.datasource.schema.Schema;

import java.util.List;

/**
 * Created by sangdn on 12/30/15.
 */
public abstract class DataSource implements IDataSource {
    protected List<String> paths;
    protected IFileFormat fileFormat;
    protected  Schema schema;
    protected  String ddfName;
    protected  long createdTime;

    public List<String> getPaths() {
        return paths;
    }

    public IFileFormat getFileFormat() {
        return fileFormat;
    }

    /**
     * @see io.ddf2.datasource.IDataSource#getSchema()
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * @see io.ddf2.datasource.IDataSource#getDDFName()
     */
    public String getDDFName() {
        return ddfName;
    }

    /**
     * @see io.ddf2.datasource.IDataSource#getNumColumn()
     */
    public abstract int getNumColumn();

    /**
     * @see io.ddf2.datasource.IDataSource#getCreatedTime()
     */
    public long getCreatedTime() {
        return createdTime;
    }

}
