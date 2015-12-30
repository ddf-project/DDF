package io.ddf2.datasource.filesystem;

import io.ddf2.datasource.IDataSource;

public class S3DataSource extends FileDataSource {

    /**
     * @see IDataSource#getNumColumn()
     */
    @Override
    public int getNumColumn() {
        return 0;
    }
}
 
