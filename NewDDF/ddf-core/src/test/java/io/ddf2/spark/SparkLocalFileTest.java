package io.ddf2.spark;

import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.fileformat.TextFileFormat;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import utils.TestUtils;

import java.util.Collections;

/**
 * Created by sangdn on 1/5/16.
 */
public class SparkLocalFileTest {
    static String pathUserData;
    @BeforeClass
    public static void before(){
        pathUserData = "/tmp/userinfo.dat";
        if(!TestUtils.makeFileUserInfo(pathUserData,10,TestUtils.TAB_SEPARATOR)){
            System.exit(1);
        }

    }
    @AfterClass
    public static void  after(){
//        File fileUser = new File(pathUserData);
//        if(fileUser.exists()) fileUser.delete();
    }
    @Test
    public void testLocalFileDSWithoutSchema() throws Exception {

        IDDFManager ddfManager = DDFManager.getInstance(SparkDDFManager.class, Collections.emptyMap());
        LocalFileDataSource localFileDataSource = LocalFileDataSource.builder()
                                                                    .addPath(pathUserData)
                                                                    .setFileFormat(new TextFileFormat(TextFileFormat.TAB_SEPARATOR))
                                                                    .build();
        ddfManager.getDDFMetaData().dropDDF("DDF_USER_INFO");
        IDDF ddf = ddfManager.newDDF("DDF_USER_INFO", localFileDataSource);
        ISqlResult sql = ddf.sql("select * from " + ddf.getDDFName());

        System.out.println("----------- Infer Schema --------- ");
        System.out.println(sql.getSchema().toString());
        System.out.println("----------- Data Result --------- ");
        String outputFormat = "Name: %10s Age: %5d isMarried %5s Birthday %20s";
        while(sql.next()){
            try {
                System.out.println("Raw: " + sql.getRaw().toString());
                String tmp = String.format(outputFormat, sql.getString(0), sql.getLong(1), sql.getBoolean(2).toString(), sql.getString(3));
                System.out.println(tmp);
            }catch(Exception ex){
                System.out.println(ex.toString());
            }
        }
        System.out.println(":: End Test Local File DataSource Without Schema::");


    }

    @Test
    public void testLocalFileDSAndSchema() throws Exception {

        IDDFManager ddfManager = DDFManager.getInstance(SparkDDFManager.class, Collections.emptyMap());
        Schema schemaUserInfo = Schema.builder() //.add(username string,age int, isMarried bool,birthday date)
                                            .add("username string")
                                            .add("age", Integer.class)
                                            .add("isMarried bool,birthday date")
                                            .build();
        IDataSource localFileDataSource = LocalFileDataSource.builder()
                .addPath(pathUserData)
                .setFileFormat(new TextFileFormat(TextFileFormat.TAB_SEPARATOR))
                .setSchema(schemaUserInfo)
                .build();
        ddfManager.getDDFMetaData().dropDDF("DDF_USER_INFO");
        IDDF ddf = ddfManager.newDDF("DDF_USER_INFO", localFileDataSource);
        ISqlResult sql = ddf.sql("select username as name,age,isMarried,birthday from " + ddf.getDDFName());

        System.out.println("----------- Infer Schema --------- ");
        System.out.println(sql.getSchema().toString());
        System.out.println("----------- Data Result --------- ");
        String outputFormat = "%10s \t %5d \t %5s \t %20s";
        System.out.println(String.format("%10s \t %5s \t %5s \t %20s","name","age","isMarried","birthday"));
        while(sql.next()){
            try {
//                System.out.println("Raw: " + sql.getRaw().toString());
                String tmp = String.format(outputFormat, sql.getString(0), sql.getInt(1), sql.getBoolean(2).toString(), sql.getDate(3));
                System.out.println(tmp);
            }catch(Exception ex){
                System.out.println(ex.toString());
            }
        }
        System.out.println(":: End Test Local File DataSource Without Schema::");


    }

    @Test
    public void testGetDDFManagerId() throws Exception {

    }

    @Test
    public void testNewDDF1() throws Exception {

    }
}