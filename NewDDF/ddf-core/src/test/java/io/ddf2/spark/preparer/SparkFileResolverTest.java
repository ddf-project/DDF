package io.ddf2.spark.preparer;

import io.ddf2.datasource.fileformat.TextFileFormat;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchema;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.Test;
import utils.TestUtils;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 1/7/16.
 */
public class SparkFileResolverTest {

    @Test
    public void testResolveLocalFileDataSource() throws Exception {
        HiveContext hiveContext = TestUtils.getHiveContext();
        SparkFileResolver resolver = new SparkFileResolver(null,hiveContext);
        String pathUserData = "/tmp/Data4SparkResolver.dat";
        TestUtils.makeFileUserInfo(pathUserData,10,TestUtils.COMMA_SEPARATOR);


        LocalFileDataSource localFileDataSource = LocalFileDataSource.builder()
                .addPath(pathUserData)
                .setFileFormat(new TextFileFormat(TextFileFormat.COMMA_SEPARATOR))
                .build();
        ISchema schema = resolver.resolve(localFileDataSource);
        System.out.println("Schema After Resolver");
        System.out.println(schema);


    }
}