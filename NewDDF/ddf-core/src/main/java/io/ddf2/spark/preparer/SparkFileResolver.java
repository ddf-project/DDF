package io.ddf2.spark.preparer;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.fileformat.IFileFormat;
import io.ddf2.datasource.fileformat.JSonFile;
import io.ddf2.datasource.fileformat.ParquetFile;
import io.ddf2.datasource.fileformat.TextFileFormat;
import io.ddf2.datasource.filesystem.FileDataSource;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchemaResolver;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.SchemaException;
import io.ddf2.spark.SparkUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by sangdn on 1/7/16.
 * <p>
 * Using Spark To InferSchema from TextDataSource (LocalFile,HDFS &S3)
 * Support IFileFortmat of text & json & sql
 */
public class SparkFileResolver implements ISchemaResolver {

    protected SparkContext sparkContext;
    protected HiveContext hiveContext;


    public SparkFileResolver(SparkContext sparkContext, HiveContext hiveContext) {
        this.sparkContext = sparkContext;
        this.hiveContext = hiveContext;
    }

    @Override
    public ISchema resolve(IDataSource dataSource) throws Exception {
        if (dataSource.getClass().isAssignableFrom(LocalFileDataSource.class)) {
            LocalFileDataSource localFileDataSource = (LocalFileDataSource) dataSource;
            return resolveFileDataSource(localFileDataSource);
        } else {
            throw new PrepareDataSourceException(dataSource);
        }

    }

    protected ISchema resolveFileDataSource(LocalFileDataSource localFileDataSource) throws PrepareDataSourceException {
        String path = localFileDataSource.getPaths().get(0);
        Class<? extends IFileFormat> fileFormat = localFileDataSource.getFileFormat().getClass();
        if (fileFormat.isAssignableFrom(TextFileFormat.class)) {
            return resolveTextFileFormat(localFileDataSource);
        } else if (fileFormat.isAssignableFrom(JSonFile.class)) {
            return resolveJsonFileFormat(localFileDataSource);
        }else if(fileFormat.isAssignableFrom(ParquetFile.class)){
            return resolveParquet(localFileDataSource);
        }else{
            throw new PrepareDataSourceException("Not found SchemaResolver to resolve " + fileFormat.getSimpleName());
        }

    }

    protected ISchema resolveTextFileFormat(LocalFileDataSource localFileDataSource) {
        TextFileFormat textFile = (TextFileFormat) localFileDataSource.getFileFormat();
        String sampleFile = localFileDataSource.getPaths().get(0); //ToDo: select better samplefile (not empty..)
        boolean containHeader = textFile.firstRowIsHeader();

        DataFrame load = hiveContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema","true")
                .option("header", String.valueOf(containHeader))
                .option("delimiter",textFile.getDelimiter())
                .load(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());

    }


    protected ISchema resolveJsonFileFormat(LocalFileDataSource localFileDataSource) throws PrepareDataSourceException {
        JSonFile jsonFile = (JSonFile) localFileDataSource.getFileFormat();
        String sampleFile = localFileDataSource.getPaths().get(0); //ToDo: select better samplefile (not empty..)


        DataFrame load = hiveContext.jsonFile(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());


    }

    protected ISchema resolveParquet(LocalFileDataSource localFileDataSource) throws PrepareDataSourceException {
        ParquetFile parquetFile = (ParquetFile) localFileDataSource.getFileFormat();
        String sampleFile = localFileDataSource.getPaths().get(0); //ToDo: select better samplefile (not empty..)

        DataFrame load = hiveContext.parquetFile(sampleFile);

        return SparkUtils.structTypeToSchema(load.schema());

    }

}
