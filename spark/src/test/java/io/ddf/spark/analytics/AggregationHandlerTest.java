package io.ddf.spark.analytics;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AggregationHandlerTest extends BaseTest {
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    createTableAirline();

    ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
  }


  @Test
  public void testSimpleAggregate() throws DDFException {

    // aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    // Assert.assertEquals(13, ddf.aggregate("year, month, mean(depdelay), median(arrdelay)").size());
    Assert.assertEquals(13, ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)").size());
    Assert.assertEquals(2, ddf.aggregate("year, month, min(depdelay), max(arrdelay)").get("2010,3").length);

    Assert.assertEquals(0.87, ddf.correlation("arrdelay", "depdelay"), 0.5);
    // project subset
    Assert.assertEquals(3, ddf.VIEWS.project(new String[] { "year", "month", "deptime" }).getNumColumns());
    Assert.assertEquals(5, ddf.VIEWS.head(5).size());
  }

  @Test
  public void testGroupBy() throws DDFException {
    List<String> l1 = Arrays.asList("year", "month");
    List<String> l2 = Arrays.asList("m=avg(depdelay)");
    List<String> l3 = Arrays.asList("m= stddev(arrdelay)");

    Assert.assertEquals(13, ddf.groupBy(l1, l2).getNumRows());
    Assert.assertTrue(ddf.groupBy(Arrays.asList("dayofweek"), l3).getNumRows() > 0);

    Assert.assertEquals(13, ddf.groupBy(l1).agg(l2).getNumRows());
    Assert.assertTrue(ddf.groupBy(Arrays.asList("dayofweek")).agg(l3).getNumRows() > 0);

    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics = count(*)")).getNumRows() > 0);
    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics = count(1)")).getNumRows() > 0);
    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics=count(dayofweek )")).getNumRows() > 0);
    Assert.assertTrue(ddf.groupBy(Arrays.asList("origin")).agg(Arrays.asList("metrics=avg(arrdelay )")).getNumRows() > 0);
  }


}
