package io.ddf2;

import io.ddf2.bigquery.BigQueryDDF;
import io.ddf2.spark.SparkDDF;
import io.ddf2.spark.SparkDDFManager;

/**
 * Created by sangdn on 2/23/2016.
 */
public class DDFManagerTest {

    public static void main() throws DDFException {
        SparkDDFManager sparkDDFManager = DDFManager.getInstance(SparkDDFManager.class,null);
        SparkDDF sparkDDF = sparkDDFManager.newDDF("select *");
//        BigQueryDDF bigQueryDDF= sparkDDFManager.newDDF("select *");
    }
}
