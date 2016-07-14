package io.ddf.spark.etl;


import com.google.common.base.Strings;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.ddf.spark.SparkDDFManager;
import io.ddf.spark.content.PersistenceHandler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;
import com.google.common.collect.Lists;
import scala.Tuple2;


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

  @Ignore
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

  @Ignore
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

  @Ignore
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

  @Test
  public void testSavingData() throws DDFException {
    this.createTableStocksSmall();
    DDF stocks = manager.sql2ddf("select unix_timestamp(concat(Date,' 00:00:00')) as unixts, * from stocks_small", "SparkSQL");
    String[] featureColumns = new String[] {"Open", "High", "Low", "Close", "Volume", "AdjustedClose"};
    String stockSymbolColumn = "Symbol";
    String timeStampColumn = "unixts";
    DDF ddf = ((TimeSeriesHandler) stocks.getTimeSeriesHandler()).flatten(new String[] {stockSymbolColumn},
        timeStampColumn, featureColumns);
    RDD<Row> rows = (RDD<Row>) ddf.getRepresentationHandler().get(RDD.class, Row.class);
    Row[] arrayRows = (Row[]) rows.collect();

    for(Row row: arrayRows) {
      Assert.assertEquals(row.getInt(1), 6, 0.0);
      if(row.getString(0).equals("AAL")) {
        Vector vector = (Vector) row.get(2);
        double[] arrDouble = vector.toArray();
        Assert.assertEquals(arrDouble[0], 46.419998, 0.01);
        Assert.assertEquals(arrDouble[1], 46.450001, 0.01);
        Assert.assertEquals(arrDouble[2], 45.150002, 0.01);
        Assert.assertEquals(arrDouble[3], 45.630001, 0.01);
        Assert.assertEquals(arrDouble[4], 8081400.0, 0.01);
        Assert.assertEquals(arrDouble[5], 45.505839, 0.01);

        Assert.assertEquals(arrDouble[6], 45.700001, 0.01);
        Assert.assertEquals(arrDouble[7], 45.830002, 0.01);
        Assert.assertEquals(arrDouble[8], 45.029999, 0.01);
        Assert.assertEquals(arrDouble[9], 45.599998, 0.01);
        Assert.assertEquals(arrDouble[10], 5864700.0, 0.01);
        Assert.assertEquals(arrDouble[11], 45.475918, 0.01);

        Assert.assertEquals(arrDouble[12], 45.509997999999996, 0.01);
        Assert.assertEquals(arrDouble[13], 45.970001, 0.01);
        Assert.assertEquals(arrDouble[14], 45.049999, 0.01);
        Assert.assertEquals(arrDouble[15], 45.34, 0.01);
        Assert.assertEquals(arrDouble[16], 11669600.0, 0.01);
        Assert.assertEquals(arrDouble[17], 45.216627, 0.01);
        Assert.assertEquals(arrDouble.length, 24, 0.0);
      }

      if(row.getString(0).equals("AAON")) {
        Vector vector = (Vector) row.get(2);
        double[] arrDouble = vector.toArray();
        Assert.assertEquals(arrDouble[0], 22.74, 0.01);
        Assert.assertEquals(arrDouble[1], 24.58, 0.01);
        Assert.assertEquals(arrDouble[2], 22.74, 0.01);
        Assert.assertEquals(arrDouble[3], 24.48, 0.01);
        Assert.assertEquals(arrDouble[4], 303100.0, 0.01);
        Assert.assertEquals(arrDouble[5], 24.372931, 0.01);

        Assert.assertEquals(arrDouble[6], 24.42, 0.01);
        Assert.assertEquals(arrDouble[7], 24.42, 0.01);
        Assert.assertEquals(arrDouble[8], 23.83, 0.01);
        Assert.assertEquals(arrDouble[9], 24.4, 0.01);
        Assert.assertEquals(arrDouble[10], 211700.0, 0.01);
        Assert.assertEquals(arrDouble[11], 24.293281, 0.01);

        Assert.assertEquals(arrDouble[12], 24.28000, 0.01);
        Assert.assertEquals(arrDouble[13], 24.5, 0.01);
        Assert.assertEquals(arrDouble[14], 23.4, 0.01);
        Assert.assertEquals(arrDouble[15], 24.23, 0.01);
        Assert.assertEquals(arrDouble[16], 240700.0, 0.01);
        Assert.assertEquals(arrDouble[17], 24.124024, 0.01);
        Assert.assertEquals(arrDouble.length, 24, 0.0);
      }
    }
  }
}
