package io.ddf.spark.analytics;


import io.ddf.DDF;
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
        .sql2ddf("select year, month, dayofweek, deptime, arrtime," +
                "origin, distance, arrdelay, depdelay, carrierdelay," +
                " weatherdelay, nasdelay, securitydelay, " +
                "lateaircraftdelay from airline", "SparkSQL");
    ddf1 = manager.sql2ddf("select year, month, dayofweek, deptime, arrdelay from airline", "SparkSQL");
  }

  @Test
  public void testSummary() throws DDFException {
    Assert.assertEquals(14, ddf.getSummary().length);
    Assert.assertEquals(31, ddf.getNumRows());
    createTableSmiths2();
    DDF ddf3 = manager.sql2ddf("select * from smiths2", "SparkSQL");
    Summary[] summary = ddf3.getSummary();
    Assert.assertEquals(summary[2].NACount(), 4);
  }

  @Test
  public void testSummaryBigInt() throws DDFException {
    DDF ddf4 = manager.sql2ddf("select floor(deptime/100) as dephour, cast(arrdelay as " +
            "bigint) as arrdelay1 from airline", "SparkSQL");
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
    DDF ddf2 = manager.sql2ddf("select * from airline", "SparkSQL");
    Assert.assertEquals(25, ddf2.VIEWS.getRandomSample(25).size());
    SparkDDF sampleDDF = (SparkDDF) ddf2.VIEWS.getRandomSample(0.5, false, 1);
    Assert.assertEquals(25, ddf2.VIEWS.getRandomSample(25).size());
    Assert.assertTrue(sampleDDF.getRDD(Object[].class).count() > 10);
  }

  @Test
  public void testVectorVariance() throws DDFException {
    DDF ddf2 = manager.sql2ddf("select * from airline", "SparkSQL");
    Double[] a = ddf2.getVectorVariance("year");
    assert (a != null);
    assert (a.length == 2);
  }

  @Test
  public void testVectorMean() throws DDFException {
    DDF ddf2 = manager.sql2ddf("select * from airline", "SparkSQL");
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
    System.out.println(">>>>> testVectorQuantiles");
    // Double[] quantiles = ddf1.getVectorQuantiles("deptime", {0.3, 0.5, 0.7});
    Double[] pArray = { 0.3, 0.5, 0.7, 1.0 };

    Double[] expectedQuantiles = { 801.0, 1416.0, 1644.0, 2107.0 };
    Double[] quantiles = ddf1.getVectorQuantiles("deptime", pArray);
    System.out.println("Quantiles: " + StringUtils.join(quantiles, ", "));
    Assert.assertArrayEquals(expectedQuantiles, quantiles);
  }

  @Test
  public void testVectorQuantilesWithDecimalCol() throws DDFException {
    System.out.println(">>>>> testVectorQuantiles with ApproxQuantile for Decimal columns");

    createTableMtcars();
    DDF ddf_movie = manager.sql2ddf("select * from mtcars", "SparkSQL");

    Double[] pArray = { 0.0, 0.3, 0.5, 0.3, 1.0};
    Double[] quantiles = ddf_movie.getVectorQuantiles("mpg", pArray);
    //System.out.println("Quantiles of " + StringUtils.join(pArray, ", ") + ": " + StringUtils.join(quantiles, ", "));
    Assert.assertArrayEquals(new Double[]{10.4, 15.68, 18.95, 15.68, 33.9}, quantiles);

    // check if wrong quantile for 0.0
    Double[] pArray1 = {0.0, 0.3};
    quantiles = ddf_movie.getVectorQuantiles("mpg", pArray1);
    //System.out.println("Quantiles of " + StringUtils.join(pArray1, ", ") + ": " + StringUtils.join(quantiles, ", "));
    Assert.assertArrayEquals(new Double[]{10.4, 15.68}, quantiles);

    // check if causing exception
    Double[] pArray2 = { 1.0 , 0.3}; // {0.1, 0.0, 0.3}
    quantiles = ddf_movie.getVectorQuantiles("mpg", pArray2);
    //System.out.println("Quantiles of " + StringUtils.join(pArray2, ", ") + ": " + StringUtils.join(quantiles, ", "));
    Assert.assertArrayEquals(new Double[]{33.9, 15.68}, quantiles);
  }

  @Test
  public void testVectorApproxHistogram() throws DDFException {
    System.out.println(">>>>> testVectorApproxHistogram");
    List<HistogramBin> bins = ddf1.getVectorApproxHistogram("arrdelay", 5);
    //for (int i = 0; i < bins.size(); i++)
    //  System.out.println(bins.get(i).getX() + " - " + bins.get(i).getY());

    Assert.assertEquals(5, bins.size());
    Assert.assertEquals(-12.45, bins.get(0).getX(), 0.01);
    Assert.assertEquals(11, bins.get(0).getY(), 0.01);

  }

  @Test
  public void testVectorHistogram() throws DDFException {
    System.out.println(">>>>> testVectorHistogram");
    List<HistogramBin> bins = ddf1.getVectorHistogram("arrdelay", 5);
    //for (int i = 0; i < bins.size(); i++)
    //  System.out.println(bins.get(i).getX() + " - " + bins.get(i).getY());

    Assert.assertEquals(5, bins.size());
    Assert.assertEquals(-24, bins.get(0).getX(), 0.01);
    Assert.assertEquals(10, bins.get(0).getY(), 0.01);
  }
}
