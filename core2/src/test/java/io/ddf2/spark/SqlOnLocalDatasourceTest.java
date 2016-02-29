package io.ddf2.spark;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.filesystem.fileformat.JSonFile;
import io.ddf2.datasource.filesystem.fileformat.ParquetFile;
import io.ddf2.datasource.filesystem.fileformat.CSVFile;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.Schema;
import org.junit.Test;
import utils.TestUtils;

import java.util.Arrays;

/**
 * Created by sangdn on 1/5/16.
 * Test SparkEngine when working on LocalDataSource
 */
public class SqlOnLocalDatasourceTest {

    IDDFManager ddfManager = SparkTestUtils.getSparkDDFManager();
    int numOfLine = 10;

    @Test
    public void testLocalCSV() throws Exception {

        String csvFile = "/tmp/userInfo.csv";
        TestUtils.makeCSVFileUserInfo(csvFile, numOfLine, TestUtils.TAB_SEPARATOR);
        LocalFileDataSource csvDataSource = LocalFileDataSource.builder().addPath(csvFile).setFileFormat(new CSVFile(CSVFile.TAB_SEPARATOR)).build();
        BaseCommonTest.TestSql(ddfManager, csvDataSource);
    }
    @Test
    public void testLocalJSON() throws Exception {
        String jsonFile = "/tmp/userInfo.json";
        TestUtils.makeJSONFileUserInfo(jsonFile, numOfLine);
        LocalFileDataSource jsonDataSource = LocalFileDataSource.builder().addPath(jsonFile).setFileFormat(new JSonFile()).build();
        BaseCommonTest.TestSql(ddfManager, jsonDataSource);

    }
    @Test
    public void testLocalParquet() throws Exception {
        String parquetFile = "/tmp/userInfo-pqt"; //parquetFile is directory
        SparkTestUtils.makeParquetFileUserInfo(SparkTestUtils.getHiveContext(), parquetFile, numOfLine);
        LocalFileDataSource pqtDataSource = LocalFileDataSource.builder().addPath(parquetFile).setFileFormat(new ParquetFile()).build();
        BaseCommonTest.TestSql(ddfManager, pqtDataSource);
    }

    @Test
    public void testLocalFileDSAndSchema() throws Exception {
        String csvFile1 = "/tmp/userInfo1.csv";
        String csvFile2 = "/tmp/userInfo2.csv";
        TestUtils.makeCSVFileUserInfo(csvFile1, 10, TestUtils.TAB_SEPARATOR);
        TestUtils.makeCSVFileUserInfo(csvFile2, 10, TestUtils.TAB_SEPARATOR);

        Schema schemaUserInfo = Schema.builder().build("username string,age int,married bool,birthday date");

        IDataSource localFileDataSource = LocalFileDataSource.builder()
                .addPath(csvFile1)
                .addPath(csvFile2)
                .setFileFormat(new CSVFile(CSVFile.TAB_SEPARATOR))
                .setSchema(schemaUserInfo)
                .build();

        ddfManager.getDDFMetaData().dropDDF("DDF_USER_INFO");
        DDF ddf = ddfManager.newDDF("DDF_USER_INFO", localFileDataSource);
//        SparkDDF ddf = ddfManager.newDDF("DDF_USER_INFO", localFileDataSource);

        ISqlResult sql = ddf.sql("select username as name,age,married,birthday from " + ddf.getDDFName());
        while(sql.next()){
            System.out.println(sql.getRaw());
        }




    }

}