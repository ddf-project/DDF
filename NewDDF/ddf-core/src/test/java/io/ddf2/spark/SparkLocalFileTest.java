package io.ddf2.spark;

import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.fileformat.TextFileFormat;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import utils.TestUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;

/**
 * Created by sangdn on 1/5/16.
 */
public class SparkLocalFileTest {
    static String pathUserData;
    @BeforeClass
    public static void before(){
        pathUserData = "/tmp/userinfo.dat";
        File fileUser = new File(pathUserData);
        if(fileUser.exists()) fileUser.delete();
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(fileUser))){
            int numLine = 10;
            for(int i = 0; i < numLine; ++i) {
                //user info schema
                //username age isMarried birthday
                bw.write(TestUtils.generateUserInfo());
                bw.newLine();
            }
        }catch (Exception ex){
            System.err.println(ex.getMessage());
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
                                                                    .setFileFormat(new TextFileFormat(TextFileFormat.COMMA_SEPARATOR))
                                                                    .build();

        IDDF ddf = ddfManager.newDDF("tblUserInfo3", localFileDataSource);
        ISqlResult sql = ddf.sql("select * from " + ddf.getDDFName());
        System.out.println("----------- Data Schema --------- ");
        System.out.println(sql.getSchema().toString());
        System.out.println("----------- Data Result --------- ");
        String outputFormat = "Name: %10s Age: %5d isMarried %5s Birthday %20s";
        while(sql.next()){
            try {
                System.out.println("Raw: " + sql.getRaw().toString());
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