package io.ddf2.datasource;

import io.ddf2.UnsupportedDataSourceException;

import java.util.List;
import java.util.Set;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Take responsible to resolve a concrete DataSource
 */
public interface IDataSourceResolver {
    void resolve(IDataSource dataSource) throws UnsupportedDataSourceException;
}
