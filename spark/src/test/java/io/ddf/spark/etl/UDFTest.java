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
    DateTime dt = formatter.parseDateTime("2015-01-22 20:23:00 +0000");

    // Make sure the output of data_parse is consumable to other UDFs
    DDF ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'hour') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'quarter') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'weekofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'dayofmonth') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());

    // Support unixtimestamp in int values
    dt = new DateTime(1433386800 * 1000L);
    ddf3 = ddf.sql2ddf("select extract(1433386800, 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'weekofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'quarter') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'dayofmonth') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());

    //Support unixtimestamp in long values
    dt = new DateTime(4147483647L * 1000);
    ddf3 = ddf.sql2ddf("select extract(4147483647, 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'weekofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'quarter') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'dayofmonth') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());

    // Support date type
    dt = new DateTime(new java.sql.Date(1433386800 * 1000L));
    System.out.println(dt.toString("yyyy-MM-dd HH:mm:ss"));
    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'weekofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'quarter') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract(to_date(from_unixtime(1433386800)), 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    // Support timestamp type
    dt = new DateTime(new java.sql.Timestamp(1433386800 * 1000L));
    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'weekofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'quarter') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select extract(cast(from_unixtime(1433386800) as timestamp), 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());
  }

  @Test
  public void testIndividualDateTimeExtractUDFs() throws DDFException {

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z");
    DateTime dt = formatter.parseDateTime("2015-01-22 20:23:00 +0000");

    // Make sure the output of data_parse is consumable to other UDFs
    DDF ddf3 = ddf.sql2ddf("select year('2015-01-22 20:23 +0000') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getYear());

    ddf3 = ddf.sql2ddf("select quarter('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select month('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select month_text('2015-01-22 20:23 +0000', 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getMonthOfYear()));

    ddf3 = ddf.sql2ddf("select month_text('2015-01-22 20:23 +0000', 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsText()));

    ddf3 = ddf.sql2ddf("select month_text('2015-01-22 20:23 +0000', 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsShortText()));

    ddf3 = ddf.sql2ddf("select weekyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select weekofyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select day('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofmonth('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select dayofweek_text('2015-01-22 20:23 +0000', 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getDayOfWeek()));

    ddf3 = ddf.sql2ddf("select dayofweek_text('2015-01-22 20:23 +0000', 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsText()));

    ddf3 = ddf.sql2ddf("select dayofweek_text('2015-01-22 20:23 +0000', 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsShortText()));

    ddf3 = ddf.sql2ddf("select dayofyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select hour('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select minute('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select second('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select millisecond('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == dt.getMillisOfSecond());

    // Support unixtimestamp in int values
    dt = new DateTime(1433386800 * 1000L);
    ddf3 = ddf.sql2ddf("select year(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select weekyear(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select weekofyear(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select month(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select month_text(1433386800, 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getMonthOfYear()));

    ddf3 = ddf.sql2ddf("select month_text(1433386800, 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsText()));

    ddf3 = ddf.sql2ddf("select month_text(1433386800, 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsShortText()));

    ddf3 = ddf.sql2ddf("select quarter(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select day(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek_text(1433386800, 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getDayOfWeek()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(1433386800, 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsText()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(1433386800, 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsShortText()));

    ddf3 = ddf.sql2ddf("select dayofmonth(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select dayofyear(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select hour(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select minute(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select second(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select millisecond(1433386800) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());

    //Support unixtimestamp in long values
    dt = new DateTime(4147483647L * 1000);
    ddf3 = ddf.sql2ddf("select year(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select weekyear(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select weekofyear(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select month(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select month_text(4147483647, 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getMonthOfYear()));

    ddf3 = ddf.sql2ddf("select month_text(4147483647, 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsText()));

    ddf3 = ddf.sql2ddf("select month_text(4147483647, 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsShortText()));

    ddf3 = ddf.sql2ddf("select quarter(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select day(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofmonth(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select dayofweek_text(4147483647, 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getDayOfWeek()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(4147483647, 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsText()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(4147483647, 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsShortText()));

    ddf3 = ddf.sql2ddf("select dayofyear(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select hour(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select minute(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select second(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select millisecond(4147483647) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());

    // Support date type
    dt = new DateTime(new java.sql.Date(1433386800 * 1000L));
    System.out.println(dt.toString("yyyy-MM-dd HH:mm:ss"));
    ddf3 = ddf.sql2ddf("select year(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select weekyear(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select weekofyear(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select month(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select month_text(to_date(from_unixtime(1433386800)), 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getMonthOfYear()));

    ddf3 = ddf.sql2ddf("select month_text(to_date(from_unixtime(1433386800)), 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsText()));

    ddf3 = ddf.sql2ddf("select month_text(to_date(from_unixtime(1433386800)), 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsShortText()));

    ddf3 = ddf.sql2ddf("select quarter(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select day(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select dayofweek_text(to_date(from_unixtime(1433386800)), 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getDayOfWeek()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(to_date(from_unixtime(1433386800)), 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsText()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(to_date(from_unixtime(1433386800)), 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsShortText()));

    ddf3 = ddf.sql2ddf("select dayofyear(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select hour(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select minute(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select second(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select millisecond(to_date(from_unixtime(1433386800))) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    // Support timestamp type
    dt = new DateTime(new java.sql.Timestamp(1433386800 * 1000L));
    ddf3 = ddf.sql2ddf("select year(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getYear());

    ddf3 = ddf.sql2ddf("select weekyear(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekyear());

    ddf3 = ddf.sql2ddf("select weekofyear(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getWeekOfWeekyear());

    ddf3 = ddf.sql2ddf("select month(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMonthOfYear());

    ddf3 = ddf.sql2ddf("select month_text(cast(from_unixtime(1433386800) as timestamp), 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getMonthOfYear()));

    ddf3 = ddf.sql2ddf("select month_text(cast(from_unixtime(1433386800) as timestamp), 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsText()));

    ddf3 = ddf.sql2ddf("select month_text(cast(from_unixtime(1433386800) as timestamp), 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.monthOfYear().getAsShortText()));

    ddf3 = ddf.sql2ddf("select quarter(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== Utils.getQuarter(dt));

    ddf3 = ddf.sql2ddf("select day(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfMonth());

    ddf3 = ddf.sql2ddf("select dayofweek(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfWeek());

    ddf3 = ddf.sql2ddf("select dayofweek_text(cast(from_unixtime(1433386800) as timestamp), 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("" + dt.getDayOfWeek()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(cast(from_unixtime(1433386800) as timestamp), 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsText()));

    ddf3 = ddf.sql2ddf("select dayofweek_text(cast(from_unixtime(1433386800) as timestamp), 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase(dt.dayOfWeek().getAsShortText()));

    ddf3 = ddf.sql2ddf("select dayofyear(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getDayOfYear());

    ddf3 = ddf.sql2ddf("select hour(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getHourOfDay());

    ddf3 = ddf.sql2ddf("select minute(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMinuteOfHour());

    ddf3 = ddf.sql2ddf("select second(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getSecondOfMinute());

    ddf3 = ddf.sql2ddf("select millisecond(cast(from_unixtime(1433386800) as timestamp)) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== dt.getMillisOfSecond());
  }
}
