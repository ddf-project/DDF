package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.joda.time.DateTime;
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
    DDF ddf2 = ddf.Transform.transformUDF("dt=concat(year,'-',month,'-',dayofmonth,' 00:00:00')");
    DDF ddf3 = ddf2.sql2ddf("select date_parse(dt, 'yyyy-MM-dd HH:mm:ss') from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    //System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase("2008-01-03 00:00:00"));

    ddf3 = ddf2.sql2ddf("select date_parse('2015-01-22 20:23 +0000', \"yyyy-MM-dd HH:mm Z\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase((DateTimeFormat.forPattern("yyyy-MM-dd HH:mm Z").parseDateTime("2015-01-22 20:23 +0000"))
        .toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testDateParseISO() throws DDFException {
    DDF ddf2 = ddf.Transform.transformUDF("dt=concat(year,'-',month,'-',dayofmonth,'T00:00:00+00')");
    DDF ddf3 = ddf2.sql2ddf("select date_parse(dt, \"yyyy-MM-dd'T'HH:mm:ssZ\") from @this");
    List<String> rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase((new DateTime("2008-01-03T00:00:00+00")).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))));

    ddf3 = ddf2.sql2ddf("select date_parse(dt, \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    //System.out.println(rows.get(0));
    Assert.assertTrue(rows.get(0).equalsIgnoreCase((new DateTime("2008-01-03T00:00:00+00")).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))));

    ddf3 = ddf2.sql2ddf("select date_parse('2015-03-20T15:04:37.617Z', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase((new DateTime("2015-03-20T15:04:37.617Z")).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))));

    ddf3 = ddf2.sql2ddf("select date_parse('2015-06-05T01:05:00-0500', \"iso\") from @this");
    rows = ddf3.VIEWS.head(1);
    Assert.assertTrue(rows.get(0).equalsIgnoreCase((new DateTime("2015-06-05T01:05:00-0500")).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))));
  }
}
