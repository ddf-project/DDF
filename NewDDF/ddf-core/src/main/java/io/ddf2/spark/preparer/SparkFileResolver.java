package io.ddf2.spark.preparer;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.filesystem.fileformat.IFileFormat;
import io.ddf2.datasource.filesystem.fileformat.JSonFile;
import io.ddf2.datasource.filesystem.fileformat.ParquetFile;
import io.ddf2.datasource.filesystem.fileformat.CSVFile;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchemaResolver;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.spark.SparkUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by sangdn on 1/7/16.
 * <p>
 * Using Spark To InferSchema from TextDataSource (LocalFile,HDFS &S3)
 * Support IFileFortmat of text & json & sql
 */
public class SparkFileResolver implements ISchemaResolver {

    protected HiveContext hiveContext;


    public SparkFileResolver(HiveContext hiveContext) {
        this.hiveContext = hiveContext;
    }

    @Override
    public ISchema resolve(IDataSource dataSource) throws Exception {
        if (dataSource.getClass().isAssignableFrom(LocalFileDataSource.class)) {
            LocalFileDataSource LocalFileDataSource = (LocalFileDataSource) dataSource;
            return resolveFileDataSource(LocalFileDataSource);
        } else {
            throw new PrepareDataSourceException(dataSource);
        }

    }

    protected ISchema resolveFileDataSource(LocalFileDataSource LocalFileDataSource) throws PrepareDataSourceException {
        String path = LocalFileDataSource.getPaths().get(0);
        Class<? extends IFileFormat> fileFormat = LocalFileDataSource.getFileFormat().getClass();
        if (fileFormat.isAssignableFrom(CSVFile.class)) {
            return resolveTextFileFormat(LocalFileDataSource);
        } else if (fileFormat.isAssignableFrom(JSonFile.class)) {
            return resolveJsonFileFormat(LocalFileDataSource);
        }else if(fileFormat.isAssignableFrom(ParquetFile.class)){
            return resolveParquet(LocalFileDataSource);
        }else{
            throw new PrepareDataSourceException("Not found SchemaResolver to resolve " + fileFormat.getSimpleName());
        }

    }

    protected ISchema resolveTextFileFormat(LocalFileDataSource LocalFileDataSource) {
        CSVFile textFile = (CSVFile) LocalFileDataSource.getFileFormat();
        String sampleFile = LocalFileDataSource.getPaths().get(0);
        boolean containHeader = textFile.firstRowIsHeader();

        DataFrame load = hiveContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema","true")
                .option("header", String.valueOf(containHeader))
                .option("delimiter",textFile.getDelimiter())
                .load(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());

    }


    protected ISchema resolveJsonFileFormat(LocalFileDataSource LocalFileDataSource) throws PrepareDataSourceException {
        JSonFile jsonFile = (JSonFile) LocalFileDataSource.getFileFormat();
        String sampleFile = LocalFileDataSource.getPaths().get(0); //ToDo: select better samplefile (not empty..)


        DataFrame load = hiveContext.jsonFile(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());


    }

    protected ISchema resolveParquet(LocalFileDataSource LocalFileDataSource) throws PrepareDataSourceException {
        ParquetFile parquetFile = (ParquetFile) LocalFileDataSource.getFileFormat();
        String sampleFile = LocalFileDataSource.getPaths().get(0); //ToDo: select better samplefile (not empty..)

        DataFrame load = hiveContext.parquetFile(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());

    }

}
