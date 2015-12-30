package io.newddf;

import io.newddf.spark.SparkDDF;
import io.newddf.spark.SparkDDFManager;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Created by sangdn on 12/21/15.
 */
public class BasicDDFManagerTest {

    @Test
    public void testNewConcreteDDFManagerImpl(){
        SparkDDFManager concreteDDFManager = DDFManager.newInstance(SparkDDFManager.class, Collections.EMPTY_MAP);
        assert concreteDDFManager != null;
    }

    @Test
    public void testInitDDF(){
        Map<String,Object> mapSparkManagerProperties = Collections.emptyMap();
        SparkDDFManager sparkDDFManager = DDFManager.newInstance(SparkDDFManager.class, mapSparkManagerProperties);
        SparkDDF sparkDDF = sparkDDFManager.newDDF(new SparkSqlDataSource("select * from tblAirline"));
        sparkDDF.sql("select * from {this}");

    }

}