package io.ddf2.datasource.schema;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.schema.ISchema;

import java.util.List;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Responsible to resolve @IFileFormat to schema
 */
public interface ISchemaResolver {
    ISchema resolve(IDataSource dataSource) throws Exception;
}
