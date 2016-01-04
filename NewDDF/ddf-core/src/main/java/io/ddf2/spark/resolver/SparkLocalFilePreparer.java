package io.ddf2.spark.resolver;

import io.ddf2.UnsupportedDataSourceException;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.fileformat.TextFile;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sangdn on 12/30/15.
 */

/**
 * Spark FSResolver
 *  + Do prepare for SparkDDF
 */

public class SparkLocalFilePreparer implements IDataSourcePreparer {
    protected JavaSparkContext javaSparkContext;
    public SparkLocalFilePreparer(JavaSparkContext javaSparkContext){
        javaSparkContext = javaSparkContext;
    }
    @Override
    public void prepare(IDataSource dataSource) throws UnsupportedDataSourceException{
        LocalFileDataSource fsDataSource = (LocalFileDataSource) dataSource;
        TextFile txtFormat = (TextFile)fsDataSource.getFileFormat();


    }
}
