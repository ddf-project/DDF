package io.ddf.spark.content;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.content.*;
import io.ddf.content.ViewHandler;
import io.ddf.content.ViewHandler.*;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

public class ViewHandlerTest extends BaseTest{

  @Test
  public void testRemoveColumns() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", false);
    DDF ddf0 = manager.sql2ddf("select * from airline", false);
    List<String> columns = Lists.newArrayList();
    columns.add("year");
    columns.add("month");
    columns.add("deptime");

    ddf.VIEWS.removeColumn("year");
    Assert.assertEquals(28, ddf.getNumColumns());
    ddf.VIEWS.removeColumns("deptime");
    Assert.assertEquals(27, ddf.getNumColumns());
    ddf0.VIEWS.removeColumns(columns);
    Assert.assertEquals(26, ddf0.getNumColumns());
  }

  @Test
  public void testSubsettingWithGrep() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", false);

    List<Column> columns = Lists.newArrayList();
    Column col = new Column();
    col.setName("origin");
    columns.add(col);

    Operator grep = new Operator();
    grep.setName(OperationName.grep);
    Expression[] operands = new Expression[2];
    StringVal val = new StringVal();
    val.setValue("IAD");
    operands[0] = val;
    operands[1] = col;
    grep.setOperarands(operands);

    DDF ddf2 = ddf.VIEWS.subset(columns, grep);
    Assert.assertEquals(2, ddf2.getNumRows());

  }

  @Test
  public void testSubsettingWithGrepIgnoreCase() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", false);

    List<Column> columns = Lists.newArrayList();
    Column col = new Column();
    col.setName("origin");
    columns.add(col);

    Operator grep = new Operator();
    grep.setName(OperationName.grep_ic);
    Expression[] operands = new Expression[2];
    StringVal val = new StringVal();
    val.setValue("iad");
    operands[0] = val;
    operands[1] = col;
    grep.setOperarands(operands);

    DDF ddf2 = ddf.VIEWS.subset(columns, grep);
    Assert.assertEquals(2, ddf2.getNumRows());
  }
}
