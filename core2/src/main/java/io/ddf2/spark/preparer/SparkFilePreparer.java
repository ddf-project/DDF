package io.ddf2.spark.preparer;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.ISchemaResolver;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * SparkFilePreparer will provide an template to prepare for SparkDDF from DataSource.
 * + Ensure Schema
 * + Create HiveTable
 * + Load Local Data Into HiveTable.
 */

public abstract class SparkFilePreparer implements IDataSourcePreparer {
    protected HiveContext hiveContext;

    public SparkFilePreparer( HiveContext hiveContext) {
        this.hiveContext = hiveContext;
    }

    protected ISchemaResolver getSchemaResolver() {
        return new SparkFileResolver(hiveContext);
    }

    @Override
    public IDataSource prepare(String ddfName, IDataSource dataSource) throws PrepareDataSourceException {
        try {
            LocalFileDataSource fileDataSource = (LocalFileDataSource) dataSource;
            ISchema schema = dataSource.getSchema();

            //check if schema exist, if not, inferschema.
            if (schema == null) {
                schema = getSchemaResolver().resolve(fileDataSource);
            }

            prepareData(ddfName, schema, fileDataSource);
            //build prepared already datasource

            return LocalFileDataSource.builder()
                    .addPaths(fileDataSource.getPaths())
                    .setFileFormat(fileDataSource.getFileFormat())
                    .setSchema(schema)
                    .setCreatedTime(System.currentTimeMillis())
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            throw new PrepareDataSourceException(e.getMessage());
        }

    }

    protected abstract void prepareData(String ddfName, ISchema schema, LocalFileDataSource fileDataSource) throws PrepareDataSourceException;


}
