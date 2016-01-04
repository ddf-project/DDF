package io.ddf2.datasource;

import io.ddf2.datasource.fileformat.IFileFormat;

import java.util.List;

/**
 * Created by sangdn on 12/30/15.
 */
public abstract class FileDataSource implements IDataSource {
    protected List<String> paths;
    protected IFileFormat fileFormat;


    public List<String> getPaths() {
        return paths;
    }

    public IFileFormat getFileFormat() {
        return fileFormat;
    }


    /**
     * @see IDataSource#getNumColumn()
     */
    public abstract int getNumColumn();


}
