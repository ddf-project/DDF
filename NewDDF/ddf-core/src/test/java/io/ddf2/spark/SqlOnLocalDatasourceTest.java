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
        String csvFile = "/tmp/userInfo.csv";
        TestUtils.makeCSVFileUserInfo(csvFile, 10, TestUtils.TAB_SEPARATOR);

        Schema schemaUserInfo = Schema.builder() //.add(username string,age int, isMarried bool,birthday date)
                .add("username string")
                .add("age", Integer.class)
                .add("isMarried bool,birthday date")
                .build();
        IDataSource localFileDataSource = LocalFileDataSource.builder()
                .addPath(csvFile)
                .setFileFormat(new CSVFile(CSVFile.TAB_SEPARATOR))
                .setSchema(schemaUserInfo)
                .build();
        ddfManager.getDDFMetaData().dropDDF("DDF_USER_INFO");
        IDDF ddf = ddfManager.newDDF("DDF_USER_INFO", localFileDataSource);
        ISqlResult sql = ddf.sql("select username as name,age,isMarried,birthday from " + ddf.getDDFName());

        System.out.println("----------- Infer Schema --------- ");
        System.out.println(sql.getSchema().toString());
        System.out.println("----------- Data Result --------- ");
        String outputFormat = "%10s \t %5d \t %5s \t %20s";
        System.out.println(String.format("%10s \t %5s \t %5s \t %20s", "name", "age", "isMarried", "birthday"));
        while (sql.next()) {
            try {
                String tmp = String.format(outputFormat, sql.getString(0), sql.getInt(1), sql.getBoolean(2).toString(), sql.getDate(3));
                System.out.println(tmp);
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }
        System.out.println(":: End Test Local File DataSource Without Schema::");


    }

}