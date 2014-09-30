package io.spark.ddf.analytics;


import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import io.ddf.exception.DDFException;
import io.spark.ddf.SparkDDF;
import io.spark.ddf.SparkDDFManager;
import java.util.List;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;



public class StatisticsSupporterTest {

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
    ddf1 = manager.sql2ddf("select year, month, dayofweek, deptime, arrdelay from airline");
  }

  @Test
  public void testSummary() throws DDFException {

    Assert.assertEquals(14, ddf.getSummary().length);
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(5, ddf1.getFiveNumSummary().length);
    Assert.assertEquals(FiveNumSummary.class, ddf1.getFiveNumSummary()[0].getClass());
    // manager.shutdown();
  }

  @Test
  public void testSampling() throws DDFException {
    DDF ddf2 = manager.sql2ddf("select * from airline");
    Assert.assertEquals(25, ddf2.VIEWS.getRandomSample(25).size());
    SparkDDF sampleDDF = (SparkDDF) ddf2.VIEWS.getRandomSample(0.5, false, 1);
    Assert.assertEquals(25, ddf2.VIEWS.getRandomSample(25).size());
    Assert.assertTrue(sampleDDF.getRDD(Object[].class).count() > 10);
  }

  @Test
  public void testVectorVariance() throws DDFException {
    DDF ddf2 = manager.sql2ddf("select * from airline");
    Double[] a = ddf2.getVectorVariance("year");
    assert (a != null);
    assert (a.length == 2);
  }

  @Test
  public void testVectorMean() throws DDFException {
    DDF ddf2 = manager.sql2ddf("select * from airline");
    Double a = ddf2.getVectorMean("year");
    assert (a != null);
    System.out.println(">>>>> testVectorMean = " + a);
  }

  @Test
  public void testVectorCor() throws DDFException {
    double a = ddf1.getVectorCor("year", "month");
    assert (a != Double.NaN);
    System.out.println(">>>>> testVectorCor = " + a);
  }

  @Test
  public void testVectorCovariance() throws DDFException {
    double a = ddf1.getVectorCor("year", "month");
    assert (a != Double.NaN);
    System.out.println(">>>>> testVectorCovariance = " + a);
  }

  @Test
  public void testVectorQuantiles() throws DDFException {
    // Double[] quantiles = ddf1.getVectorQuantiles("deptime", {0.3, 0.5, 0.7});
    Double[] pArray = { 0.3, 0.5, 0.7 };
    Double[] expectedQuantiles = { 801.0, 1416.0, 1644.0 };
    Double[] quantiles = ddf1.getVectorQuantiles("deptime", pArray);
    System.out.println("Quantiles: " + StringUtils.join(quantiles, ", "));
    Assert.assertArrayEquals(expectedQuantiles, quantiles);
  }

  @Test
  public void testVectorHistogram() throws DDFException {
    List<HistogramBin> bins = ddf1.getVectorHistogram("arrdelay", 5);
    Assert.assertEquals(5, bins.size());
  }

  @After
  public void closeTest() {
    manager.shutdown();
  }


}
