package io.ddf2.spark.resolver;

import io.ddf2.UnsupportedDataSourceException;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourceResolver;
import io.ddf2.datasource.filesystem.FSDataSource;
import io.ddf2.datasource.IFileFormat;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sangdn on 12/30/15.
 */
public class FileSystemResolver implements IDataSourceResolver {
    protected JavaSparkContext javaSparkContext;
    public FileSystemResolver(JavaSparkContext javaSparkContext){
        javaSparkContext = javaSparkContext;
    }
    @Override
    public void resolve(IDataSource dataSource) throws UnsupportedDataSourceException{
        FSDataSource fsDataSource = (FSDataSource) dataSource;
        IFileFormat fileFormat = fsDataSource.getFileFormat();


    }
}
