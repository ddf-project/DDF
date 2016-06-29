package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import com.google.common.collect.Lists;

public class TimeSeriesHandlerTest extends BaseTest {
  public static DDF stocks;
  public static DDF aal_stocks;


  @BeforeClass
  public static void setupTestData() throws DDFException {

    createTableStocks();
    stocks = manager.sql2ddf("select unix_timestamp(concat(Date,' 00:00:00')) as unixts, * from stocks", "SparkSQL");
    aal_stocks = manager.sql2ddf(
        "select unix_timestamp(concat(Date,' 00:00:00')) as unixts, * from stocks where Symbol='AAL'", "SparkSQL");
  }

  @Test
  public void testDownSampling() throws DDFException {

    Assert.assertTrue(aal_stocks.getNumRows() == 30);
    List<String> aggFuncs = Lists.newArrayList("close_avg = avg(Close)");
    aggFuncs.add("volume_max = max(Volume)");
    DDF downsampled_aal = aal_stocks.getTimeSeriesHandler().downsample("unixts", aggFuncs, 5, TimeUnit.DAYS);
    DDF downsampled_stocks = stocks.getTimeSeriesHandler().downsample("unixts", "symbol", aggFuncs, 5, TimeUnit.DAYS);

    Assert.assertEquals(3, downsampled_aal.getNumColumns());
    Assert.assertEquals(4, downsampled_stocks.getNumColumns());

    Assert.assertTrue(downsampled_aal.getNumRows() == 8);
    Assert.assertTrue(downsampled_stocks.getNumRows() == 80);

  }

}
