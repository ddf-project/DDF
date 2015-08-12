package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
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

    ddf = manager.sql2ddf("select * from airline");
  }

  @Test
  public void testDateParseNonISO() throws DDFException {
    DDF ddf2 = ddf.Transform.transformUDF("dt=concat(year,'/',month,'/',dayofmonth,' 00:00:00')");
    DDF ddf3 = ddf2.sql2ddf("select date_parse(dt, 'yyyy/MM/dd HH:mm:ss') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2008-01-03 00:00:00"));
  }

  @Test
  public void testDateParseISO() throws DDFException {

    DDF ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"yyyy-MM-dd HH:mm Z\") from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-01-22 20:23:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-01-22 20:23:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-03-20T15:04:37.617Z', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-03-20 15:04:37"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-06-05T01:05:00-0500', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-06-05 06:05:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-04-24 11:27:43.177', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-04-24 11:27:43"));

    ddf3 = ddf.sql2ddf("select date_parse('2014-05-01T02:30:00+00', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2014-05-01 02:30:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27 16:11:41', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-27 16:11:41"));

    ddf3 = ddf.sql2ddf("select date_parse('2013', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-01-01 00:00:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-01 00:00:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-27 00:00:00"));


    // Make sure the output of data_parse is consumable to other UDFs
    ddf3 = ddf.sql2ddf("select hour(date_parse('2015-06-05T01:05:00-0500', \"iso\")) from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 6);
  }

  @Test
  public void testDateTimeExtract() throws DDFException {
    // Make sure the output of data_parse is consumable to other UDFs
    DDF ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'hour') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 20);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2015);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2015);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'weekofweekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 4);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 1);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 22);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 4);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 22);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 23);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract('2015-01-22 20:23 +0000', 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    // Support unixtimestamp in int values
    ddf3 = ddf.sql2ddf("select extract(1433386800, 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2015);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2015);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'weekofweekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 23);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 6);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 4);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 4);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 155);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 3);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    ddf3 = ddf.sql2ddf("select extract(1433386800, 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);

    //Support unixtimestamp in long values
    ddf3 = ddf.sql2ddf("select extract(4147483647, 'year') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2101);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'weekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 2101);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'weekofweekyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 23);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'month') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 6);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'day') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 6);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'dayofweek') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 1);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'dayofyear') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 157);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'hour') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 6);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'minute') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 47);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'second') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 27);

    ddf3 = ddf.sql2ddf("select extract(4147483647, 'millisecond') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 0);
  }

  @Test
  public void testIndividualDateTimeExtractUDFs() throws DDFException {
    // Make sure the output of data_parse is consumable to other UDFs
    DDF ddf3 = ddf.sql2ddf("select year('2015-01-22 20:23 +0000') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 2015);

    ddf3 = ddf.sql2ddf("select month('2015-01-22 20:23 +0000', 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("1"));

    ddf3 = ddf.sql2ddf("select month('2015-01-22 20:23 +0000', 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("January"));

    ddf3 = ddf.sql2ddf("select month('2015-01-22 20:23 +0000', 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("Jan"));

    ddf3 = ddf.sql2ddf("select weekyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 2015);

    ddf3 = ddf.sql2ddf("select weekofweekyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 4);

    ddf3 = ddf.sql2ddf("select day('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 22);

    ddf3 = ddf.sql2ddf("select dayofweek('2015-01-22 20:23 +0000', 'number') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("4"));

    ddf3 = ddf.sql2ddf("select dayofweek('2015-01-22 20:23 +0000', 'text') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("Thursday"));

    ddf3 = ddf.sql2ddf("select dayofweek('2015-01-22 20:23 +0000', 'shorttext') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("Thu"));

    ddf3 = ddf.sql2ddf("select dayofyear('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 22);

    ddf3 = ddf.sql2ddf("select hour('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 20);

    ddf3 = ddf.sql2ddf("select minute('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 23);

    ddf3 = ddf.sql2ddf("select second('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 0);

    ddf3 = ddf.sql2ddf("select millisecond('2015-01-22 20:23 +0000') from @this");
    rows = ddf3.VIEWS.head(1);
    System.out.println(rows.get(0));
    Assert.assertTrue(Integer.parseInt(rows.get(0)) == 0);
  }

}
