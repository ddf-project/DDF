package io.spark.ddf.content;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.content.*;
import io.ddf.content.ViewHandler;
import io.ddf.content.ViewHandler.*;
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

  @Test
  public void testSubsettingWithGrep() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline");

    List<Column> columns = Lists.newArrayList();
    Column col = new Column();
    col.setName("origin");
    col.setType("Column");
    columns.add(col);

    Operator grep = new Operator();
    grep.setName(OperationName.grep);
    grep.setType("Operator");
    Expression[] operands = new Expression[2];
    StringVal val = new StringVal();
    val.setValue("IAD");
    val.setType("StringVal");
    operands[0] = val;
    operands[1] = col;
    grep.setOperarands(operands);

    DDF ddf2 = ddf.VIEWS.subset(columns, grep);
    Assert.assertEquals(2, ddf2.getNumRows());

  }

  @Test
  public void testSubsettingWithGrepIgnoreCase() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline");

    List<Column> columns = Lists.newArrayList();
    Column col = new Column();
    col.setName("origin");
    col.setType("Column");
    columns.add(col);

    Operator grep = new Operator();
    grep.setName(OperationName.grep_ic);
    grep.setType("Operator");
    Expression[] operands = new Expression[2];
    StringVal val = new StringVal();
    val.setValue("iad");
    val.setType("StringVal");
    operands[0] = val;
    operands[1] = col;
    grep.setOperarands(operands);

    DDF ddf2 = ddf.VIEWS.subset(columns, grep);
    Assert.assertEquals(2, ddf2.getNumRows());
  }
}
