package io.spark.ddf;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SparkDDFManagerTests extends BaseTest {

  @Test
  public void testDDFConfig() throws Exception {

    Assert.assertEquals("spark", manager.getEngine());
  }

  @Test
  public void testLongSparkDDFManagerRetrieval() throws DDFException {
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));
  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {

    createTableAirline();

    List<String> l = manager.sql2txt("select * from airline");
    Assert.assertEquals(31, l.size());

    List<String> v = manager.sql2txt("select count(*) from airline");
    Assert.assertEquals(1, v.size());
    Assert.assertEquals("31", v.get(0));

    DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
        + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");

    Assert.assertEquals(14, ddf.getSummary().length);
    Assert.assertEquals("ddf://adatao/" + ddf.getName(), ddf.getUri());

    manager.addDDF(ddf);
    Assert.assertEquals(ddf, manager.getDDF(ddf.getUri()));
  }
}
