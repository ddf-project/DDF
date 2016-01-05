package io.ddf2.datasource;

import io.ddf2.UnsupportedDataSourceException;

import java.util.List;
import java.util.Set;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Take responsible preparing for DDF
 *  + ensure schema
 *  + prepare anything for DDF to work well on this dataSource.
 */
public interface IDataSourcePreparer {
    IDataSource prepare(String ddfName,IDataSource dataSource) throws PrepareDataSourceException;
}
