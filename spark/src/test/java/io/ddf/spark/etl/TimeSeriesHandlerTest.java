package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
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

  @Test
  public void testDiff() throws DDFException {
    Assert.assertTrue(stocks.getNumRows() == 300);

    String colToGetDiff = "close";
    String diffColName = "close_diff";
    DDF aalWithDiff = aal_stocks.getTimeSeriesHandler().addDiffColumn("unixts", null, colToGetDiff, diffColName);
    Assert.assertEquals(10, aalWithDiff.getNumColumns());

    DDF ddfWithDiff = stocks.getTimeSeriesHandler().addDiffColumn("unixts", "symbol", colToGetDiff, diffColName);
    Assert.assertEquals(10, ddfWithDiff.getNumColumns());

    // List<String> rs1 = manager.sql(String.format("Select * from %s limit 12",
    // ddfWithDiff.getTableName()), "SparkSQL").getRows();
    List<String> col1 = ddfWithDiff.VIEWS.project(colToGetDiff).VIEWS.head(10);
    double expected_diff = Double.parseDouble(col1.get(1)) - Double.parseDouble(col1.get(0));
    List<String> diff_col = ddfWithDiff.VIEWS.project(diffColName).VIEWS.head(10);
    double diff = Double.parseDouble(diff_col.get(1));
    Assert.assertEquals(expected_diff, diff, 1e-4);

  }

  @Test
  public void testMovingAverage() throws DDFException {

    String colToComputeMovingAverage = "volume";
    String movingAverageColName = "movingAvg";
    int windowSize = 3;
    DDF ddfWithMA = stocks.getTimeSeriesHandler().computeMovingAverage("unixts", "symbol", colToComputeMovingAverage,
        movingAverageColName, windowSize);
    Assert.assertEquals(10, ddfWithMA.getNumColumns());

    List<String> col1 = ddfWithMA.VIEWS.project(colToComputeMovingAverage).VIEWS.head(10);
    List<String> ma_col = ddfWithMA.VIEWS.project(movingAverageColName).VIEWS.head(10);
    double expected_ma = (Double.parseDouble(col1.get(0)) + Double.parseDouble(col1.get(1)) + Double.parseDouble(col1
        .get(2))) / windowSize;
    double ma = Double.parseDouble(ma_col.get(1));

    Assert.assertEquals(expected_ma, ma, 1e-4);
  }
}
