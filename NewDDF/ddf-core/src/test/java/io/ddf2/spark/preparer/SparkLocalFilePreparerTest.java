package io.ddf2.spark.preparer;

import io.ddf2.datasource.filesystem.fileformat.CSVFile;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.TextFileSchemaResolver;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import utils.TestUtils;

import java.io.File;

/**
 * Created by sangdn on 1/6/16.
 */
public class SparkLocalFilePreparerTest {

    @Test
    public void inferCommaSchemaTest() throws Exception {
        String pathUserData = "/tmp/infer-schema-comma.dat";
        TestUtils.makeCSVFileUserInfo(pathUserData, 10, TestUtils.COMMA_SEPARATOR);

        SparkFilePreparer preparer = new LocalFilePreparer(null);
        LocalFileDataSource localFileDataSource = LocalFileDataSource.builder()
                .addPath(pathUserData)
                .setFileFormat(new CSVFile(CSVFile.COMMA_SEPARATOR))
                .build();
        TextFileSchemaResolver schemaResolver= new TextFileSchemaResolver();
        ISchema resolve = schemaResolver.resolve(localFileDataSource);
        assert resolve.getNumColumn() == 4;


        FileUtils.deleteQuietly(new File(pathUserData));


    }

}