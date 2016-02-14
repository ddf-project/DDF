package io.ddf2.bigquery;

import io.ddf2.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 2/14/16.
 */
public class BigQueryMetaDataTest {
    protected static IDDFManager ddfManager;

    /**
     * prepare env, exist data for test: https://bigquery.cloud.google.com/table/bigquery-dataproc:demo.airline_2007
     */
    @BeforeClass
    public static void beforeClass(){

        BigQueryContext.setAppName("BigQueryMetaDataTest");
        BigQueryContext.setProjectId("891318981931");
        BigQueryContext.setClientId("32555940559.apps.googleusercontent.com");
        BigQueryContext.setClientSecret("ZmssLNjJy2998hD4CTg2ejr2");
        BigQueryContext.setRefreshToken("1/4Zhx7SH9rgB6HNsK704ko1aTWyDQXbRnOZb5ojTYzuI");

        Map options = Collections.EMPTY_MAP;
        ddfManager = DDFManager.getInstance(BigQueryManager.class, options);
    }
    @Test
    public void testGetMetaDataInfo() throws DDFException {
        IDDFMetaData ddfMetaData = ddfManager.getDDFMetaData();
        BaseCommonTest.testDDFMetaData(ddfMetaData);
    }
}