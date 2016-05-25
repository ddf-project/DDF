package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import io.ddf.spark.util.Utils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created by nhanitvn on 28/07/2015.
 */
public class UDFTest extends BaseTest {
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    createTableAirlineWithNA();

    ddf = manager.sql2ddf("select * from airline", "SparkSQL");
  }

  @Test
  public void testDateParseNonISO() throws DDFException {
    DDF ddf2 = ddf.Transform.transformUDF("dt=concat(year,'/',month,'/',dayofmonth,' 00:00:00')");
    DDF ddf3 = ddf2.sql2ddf("select date_parse(dt, 'yyyy/MM/dd HH:mm:ss') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2008-01-03 00:00:00"));
  }

  @Test
  public void testDateParseISO() throws DDFException {

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z");
    DateTime dt = formatter.parseDateTime("2015-01-22 20:23:00 +0000");

    DDF ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"yyyy-MM-dd HH:mm Z\") from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    dt = formatter.parseDateTime("2015-03-20T15:04:37.617Z");
    ddf3 = ddf.sql2ddf("select date_parse('2015-03-20T15:04:37.617Z', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));


    formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
    dt = formatter.parseDateTime("2015-06-05T01:05:00-0500");
    ddf3 = ddf.sql2ddf("select date_parse('2015-06-05T01:05:00-0500', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    dt = formatter.parseDateTime("2015-04-24 11:27:43.177");
    ddf3 = ddf.sql2ddf("select date_parse('2015-04-24 11:27:43.177', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));


    formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
    dt = formatter.parseDateTime("2014-05-01T02:30:00+00");
    ddf3 = ddf.sql2ddf("select date_parse('2014-05-01T02:30:00+00', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));


    formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    dt = formatter.parseDateTime("2013-11-27 16:11:41");
    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27 16:11:41', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    formatter = DateTimeFormat.forPattern("yyyy");
    dt = formatter.parseDateTime("2013");
    ddf3 = ddf.sql2ddf("select date_parse('2013', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    formatter = DateTimeFormat.forPattern("yyyy-MM");
    dt = formatter.parseDateTime("2013-11");
    ddf3 = ddf.sql2ddf("select date_parse('2013-11', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));

    formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    dt = formatter.parseDateTime("2013-11-27");
    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.toString("yyyy-MM-dd HH:mm:ss")));


    // Make sure the output of data_parse is consumable to other UDFs
    formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
    dt = formatter.parseDateTime("2015-06-05T01:05:00-0500");
    ddf3 = ddf.sql2ddf("select hour(date_parse('2015-06-05T01:05:00-0500', \"iso\")) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());
  }

  @Test
  public void testDateTimeExtract() throws DDFException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z");
    // We test on those data types: an ISO 8601 string, an integer unix-timestamp, a long unix-timestamp, a date and a timestamp
    DateTime[] dts = {formatter.parseDateTime("2015-01-22 20:23:00 +0000"), new DateTime(1433386800 * 1000L), new DateTime(4147483647L * 1000), new DateTime("2015-06-04"), new DateTime(new java.sql.Timestamp(1433386800 * 1000L))};
    String[] sqlDts = {"'2015-01-22 20:23 +0000'", "1433386800", "4147483647", "to_date('2015-06-04')", "cast(from_unixtime(1433386800) as timestamp)"};

    for(int i = 0; i < dts.length; i++) {
      DateTime dt = dts[i];

      String[] udfs = {"year", "weekyear", "week", "weekofyear", "weekofmonth", "month", "quarter", "day", "dayofweek", "dayofyear", "hour", "minute", "second", "millisecond"};
      int[] expectedValues = {dt.getYear(), dt.getWeekyear(), dt.getWeekOfWeekyear(), dt.getWeekOfWeekyear(), (dt.getDayOfMonth() / 7) + 1, dt.getMonthOfYear(), Utils.getQuarter(dt), dt.getDayOfMonth(),
              dt.getDayOfWeek(), dt.getDayOfYear(), dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute(), dt.getMillisOfSecond()};

      for(int k = 0; k < udfs.length; k++) {
        DDF ddf2 = ddf.sql2ddf("select extract(" + sqlDts[i] + ", '" + udfs[k] + "') from @this");
        List<String> rows = ddf2.VIEWS.head(1);
        Assert.assertTrue(Integer.parseInt(rows.get(0))== expectedValues[k]);
      }

    }

  }

  @Test
  public void testIndividualDateTimeExtractUDFs() throws DDFException {

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z");
    // We test on those data types: an ISO 8601 string, an integer unix-timestamp, a long unix-timestamp, a date and a timestamp
    DateTime[] dts = {formatter.parseDateTime("2015-01-22 20:23:00 +0000"), new DateTime(1433386800 * 1000L), new DateTime(4147483647L * 1000), new DateTime("2015-06-04"), new DateTime(new java.sql.Timestamp(1433386800 * 1000L))};
    String[] sqlDts = {"'2015-01-22 20:23 +0000'", "1433386800", "4147483647", "to_date('2015-06-04')", "cast(from_unixtime(1433386800) as timestamp)"};

    for(int i = 0; i < dts.length; i++) {
      DateTime dt = dts[i];
      // UDFs to test
      String[] udfs1 = {"year", "weekyear", "week", "weekofyear", "weekofmonth", "month", "quarter", "day", "dayofweek", "dayofyear", "hour", "minute", "second", "millisecond"};
      int[] expectedValues1 = {dt.getYear(), dt.getWeekyear(), dt.getWeekOfWeekyear(), dt.getWeekOfWeekyear(), (dt.getDayOfMonth() / 7) + 1, dt.getMonthOfYear(), Utils.getQuarter(dt), dt.getDayOfMonth(),
              dt.getDayOfWeek(), dt.getDayOfYear(), dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute(), dt.getMillisOfSecond()};

      for(int k = 0; k < udfs1.length; k++) {
        DDF ddf2 = ddf.sql2ddf("select " + udfs1[k] + "(" + sqlDts[i] + ") from @this");
        List<String> rows = ddf2.VIEWS.head(1);
        Assert.assertTrue(Integer.parseInt(rows.get(0))== expectedValues1[k]);
      }

      // Test month_text and dayofweek_text separately due to their signature and return value (string)
      String[] udfs2 = {"month_text", "dayofweek_text"};
      String[] types = {"number", "text", "shorttext"};
      String[] expectedValues2 = {dt.getMonthOfYear() +  "", dt.monthOfYear().getAsText(), dt.monthOfYear().getAsShortText(), dt.getDayOfWeek() + "", dt.dayOfWeek().getAsText(), dt.dayOfWeek().getAsShortText()};
      for(int k = 0; k < udfs2.length; k++) {
        for(int l = 0; l < types.length; l++) {
          DDF ddf2 = ddf.sql2ddf("select " + udfs2[k] + "(" + sqlDts[i] + ", '" + types[l] + "') from @this");
          List<String> rows = ddf2.VIEWS.head(1);
          Assert.assertTrue(rows.get(0).equalsIgnoreCase(expectedValues2[k * 3 + l]));
        }
      }
    }
  }
}
