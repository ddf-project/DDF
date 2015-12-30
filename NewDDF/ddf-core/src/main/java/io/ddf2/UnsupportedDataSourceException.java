package io.ddf2;

import io.ddf2.datasource.IDataSource;

/**
 * Created by sangdn on 12/30/15.
 */
public class UnsupportedDataSourceException extends Exception {

    public UnsupportedDataSourceException(IDataSource dataSource){
        super("Unsupported " + dataSource.getClass().getSimpleName());
    }
}
