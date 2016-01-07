package io.ddf2.datasource;

import io.ddf2.DDFException;

/**
 * Created by sangdn on 1/4/16.
 */
public class PrepareDataSourceException extends DDFException {
    public PrepareDataSourceException(String message) {
        super(message);
    }
    public PrepareDataSourceException(IDataSource dataSource){
        super("Exception when preparing data from " + dataSource.getClass().getSimpleName());
    }
}
