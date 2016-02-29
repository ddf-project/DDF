package io.ddf2.bigquery;

import io.ddf2.*;
import io.ddf2.datasource.schema.ISchema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by sangdn on 1/26/16.
 */
public class BigQueryManagerTest {

    protected static IDDFManager ddfManager;

    /**
     * prepare env, exist data for test: https://bigquery.cloud.google.com/table/bigquery-dataproc:demo.airline_2007
     */
    @BeforeClass
    public static void beforeClass(){

        BigQueryContext.setAppName("BigQueryManagerTest");
        BigQueryContext.setProjectId("891318981931");
        BigQueryContext.setClientId("32555940559.apps.googleusercontent.com");
        BigQueryContext.setClientSecret("ZmssLNjJy2998hD4CTg2ejr2");
        BigQueryContext.setRefreshToken("1/4Zhx7SH9rgB6HNsK704ko1aTWyDQXbRnOZb5ojTYzuI");

        Map options = Collections.EMPTY_MAP;
        ddfManager = DDFManager.getInstance(BigQueryManager.class, options);
    }

    @Test
    public void testSql() throws SQLException {
        ISqlResult sqlResult = ddfManager.sql("select * from demo.airline_2007 limit 10");
        ISchema schema = sqlResult.getSchema();
        System.out.println("---------- Schema --------------------");
        System.out.println(schema.toString());
        System.out.println("----------- Top 10 Data --------------");
        while(sqlResult.next()){
            Object row = sqlResult.getRaw();
            System.out.println(row.toString());
        }
    }

    @Test
    public void testSimpleDDF() throws DDFException, SQLException {
        String ddfName = "abc123";
//        ddfManager.getDDFMetaData().dropDDF(ddfName);
        DDF ddf = ddfManager.newDDF(ddfName,"select * from demo.airline_2007 limit 10");
        ISqlResult sqlResult = ddf.sql("select * from " + ddfName);
        ISchema schema = sqlResult.getSchema();
        System.out.println("---------- Schema --------------------");
        System.out.println(schema.toString());
        System.out.println("-----------  Data --------------");
        while(sqlResult.next()){
            Object row = sqlResult.getRaw();
            System.out.println(row.toString());
        }
        assert ddf.equals(ddfManager.getDDF(ddfName));
    }









}