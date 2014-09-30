package io.spark.ddf.analytics;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.spark.ddf.SparkDDFManager;

public class AggregationHandlerTest {
  private DDFManager manager;
  private DDF ddf, ddf1;


  @Before
  public void setUp() throws Exception {
    manager = DDFManager.get("spark");
    Map<String, String> params = ((SparkDDFManager) manager).getSparkContextParams();
    System.out.println(System.getProperty("spark.serializer"));
    System.out.println(params.get("DDFSPARK_JAR"));

    manager.sql2txt("drop table if exists airline");

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/airline.csv' into table airline");

    ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
    // ddf1 = manager.sql2ddf("select year, month, dayofweek, deptime from airline");
  }


  @Test
  public void testSimpleAggregate() throws DDFException {

    // aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    Assert.assertEquals(13, ddf.aggregate("year, month, mean(depdelay), median(arrdelay)").size());
    Assert.assertEquals(2, ddf.aggregate("year, month, min(depdelay), max(arrdelay)").get("2010,3").length);

    Assert.assertEquals(0.87, ddf.correlation("arrdelay", "depdelay"), 0.0);
    // project subset
    Assert.assertEquals(3, ddf.VIEWS.project(new String[] { "year", "month", "deptime" }).getNumColumns());
    Assert.assertEquals(5, ddf.VIEWS.head(5).size());
    // manager.shutdown();
  }

  @Test
  public void testGroupBy() throws DDFException {
    List<String> l1 = Arrays.asList("year", "month");
    List<String> l2 = Arrays.asList("m=mean(depdelay)");
    List<String> l3 = Arrays.asList("m= stddev(arrdelay)");

    Assert.assertEquals(13, ddf.groupBy(l1, l2).getNumRows());
    Assert.assertTrue(ddf.groupBy(Arrays.asList("dayofweek"), l3).getNumRows() > 0);

    Assert.assertEquals(13, ddf.groupBy(l1).agg(l2).getNumRows());
    Assert.assertTrue(ddf.groupBy(Arrays.asList("dayofweek")).agg(l3).getNumRows() > 0);

    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics = count(*)")).getNumRows() > 0);
    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics = count(1)")).getNumRows() > 0);
    Assert
        .assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics=count(dayofweek )")).getNumRows() > 0);
  }

  @After
  public void closeTest() {
    manager.shutdown();
  }

}
