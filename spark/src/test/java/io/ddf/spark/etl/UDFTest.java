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
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2008-01-03 00:00:00"));
  }

  @Test
  public void testDateParseISO() throws DDFException {

    DDF ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"yyyy-MM-dd HH:mm Z\") from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-01-23 03:23:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-01-23 03:23:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-03-20T15:04:37.617Z', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-03-20 22:04:37"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-06-05T01:05:00-0500', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-06-05 13:05:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2015-04-24 11:27:43.177', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2015-04-24 11:27:43"));

    ddf3 = ddf.sql2ddf("select date_parse('2014-05-01T02:30:00+00', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2014-05-01 09:30:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27 16:11:41', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-27 16:11:41"));

    ddf3 = ddf.sql2ddf("select date_parse('2013', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-01-01 00:00:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-01 00:00:00"));

    ddf3 = ddf.sql2ddf("select date_parse('2013-11-27', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2013-11-27 00:00:00"));


    // Make sure the output of data_parse is consumable to other UDFs
    ddf3 = ddf.sql2ddf("select hour(date_parse('2015-06-05T01:05:00-0500', \"iso\")) from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(Integer.parseInt(rows.get(0))== 13);
  }
}
