package io.ddf.spark.analytics;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;
import io.ddf.analytics.Summary;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import io.ddf.spark.SparkDDF;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class StatisticsSupporterTest extends BaseTest {
  private DDF ddf, ddf1, mdf;

  @Before
  public void setUp() throws Exception {
    createTableAirline();

    ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
    ddf1 = manager.sql2ddf("select year, month, dayofweek, deptime, arrdelay from airline");
  }

  @Test
  public void testSummary() throws DDFException {
    Assert.assertEquals(14, ddf.getSummary().length);
    Assert.assertEquals(31, ddf.getNumRows());
    createTableSmiths2();
    DDF ddf3 = manager.sql2ddf("select * from smiths2");
    Summary[] summary = ddf3.getSummary();
    Assert.assertEquals(summary[2].NACount(), 4);
  }

  @Test
  public void testSummaryBigInt() throws DDFException {
    DDF ddf4 = manager.sql2ddf("select floor(deptime/100) as dephour, cast(arrdelay as bigint) as arrdelay1 from airline");
    Summary[] summary = ddf4.getSummary();
    Assert.assertTrue(summary[0].min() != Double.NaN);
    Assert.assertTrue(summary[0].count() > 0);
    Assert.assertTrue(summary[1].min() != Double.NaN);
    Assert.assertTrue(summary[1].count() > 0);
  }

//  @Test
//  public void testFiveNum() throws DDFException {
//    Assert.assertEquals(5, ddf1.getFiveNumSummary().length);
//    Assert.assertEquals(FiveNumSummary.class, ddf1.getFiveNumSummary()[0].getClass());
//  }

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

//  @Test
//  public void testVectorQuantiles() throws DDFException {
//    // Double[] quantiles = ddf1.getVectorQuantiles("deptime", {0.3, 0.5, 0.7});
//    Double[] pArray = { 0.3, 0.5, 0.7 };
//    Double[] expectedQuantiles = { 801.0, 1416.0, 1644.0 };
//    Double[] quantiles = ddf1.getVectorQuantiles("deptime", pArray);
//    System.out.println("Quantiles: " + StringUtils.join(quantiles, ", "));
//    Assert.assertArrayEquals(expectedQuantiles, quantiles);
//  }

  @Test
  public void testVectorHistogram_Hive() throws DDFException {
    List<HistogramBin> bins = ddf1.getVectorHistogram_Hive("arrdelay", 5);
    for (int i = 0; i < bins.size(); i++) {
      System.out.println(bins.get(i).getX() + " - " + bins.get(i).getY());
    }
    Assert.assertEquals(5, bins.size());
    Assert.assertEquals(-12.45, bins.get(0).getX(), 0.01);
    Assert.assertEquals(11, bins.get(0).getY(), 0.01);

  }

  @Test
  public void testVectorHistogram() throws DDFException {
    //createTableMovie();
    //mdf = manager.sql2ddf("select year, length, rating, votes from movie");
    //List<HistogramBin> bins = mdf.getVectorHistogram("length", 5);

    List<HistogramBin> bins = ddf1.getVectorHistogram("arrdelay", 5);
    for (int i = 0; i < bins.size(); i++) {
      System.out.println(bins.get(i).getX() + " - " + bins.get(i).getY());
    }
  }
}
