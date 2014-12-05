package io.spark.ddf.content;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.spark.ddf.BaseTest;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

public class ViewHandlerTest extends BaseTest{

  @Test
  public void testRemoveColumns() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline");

    List<String> columns = Lists.newArrayList();
    columns.add("year");
    columns.add("month");
    columns.add("deptime");

    DDF newddf1 = ddf.VIEWS.removeColumn("year");
    DDF newddf2 = ddf.VIEWS.removeColumns("year", "deptime");
    DDF newddf3 = ddf.VIEWS.removeColumns(columns);

    Assert.assertEquals(28, newddf1.getNumColumns());
    Assert.assertEquals(27, newddf2.getNumColumns());
    Assert.assertEquals(26, newddf3.getNumColumns());
  }
}
