package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.filesystem.fileformat.IFileFormat;

import java.util.List;

public class S3DataSource extends FileDataSource {

    /**
     * @see IDataSource#getNumColumn()
     */
    @Override
    public int getNumColumn() {
        return 0;
    }

    @Override
    public List<String> getPaths() {
        return null;
    }

    @Override
    public IFileFormat getFileFormat() {
        return null;
    }
}

