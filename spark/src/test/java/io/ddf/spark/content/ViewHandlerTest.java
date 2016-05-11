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

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");
    DDF ddf0 = manager.sql2ddf("select * from airline", "SparkSQL");
    List<String> columns = Lists.newArrayList();
    columns.add("year");
    columns.add("month");
    columns.add("deptime");

    DDF ddf1 = ddf.VIEWS.removeColumn("year");
    Assert.assertEquals(28, ddf1.getNumColumns());
    DDF ddf2 = ddf1.VIEWS.removeColumns("deptime");
    Assert.assertEquals(27, ddf2.getNumColumns());
    DDF ddf3 = ddf0.VIEWS.removeColumns(columns);
    Assert.assertEquals(26, ddf3.getNumColumns());
    Assert.assertEquals(29, ddf0.getNumColumns());
    Assert.assertEquals(29, ddf.getNumColumns());

    DDF ddf4 = ddf.VIEWS.removeColumn("year", true);
    Assert.assertEquals(28, ddf.getNumColumns());
    Assert.assertEquals(ddf, ddf4);

    DDF ddf5 = ddf0.VIEWS.removeColumns(columns, true);
    Assert.assertEquals(26, ddf5.getNumColumns());
    Assert.assertEquals(ddf5, ddf0);
  }

  @Test
  public void testSubsettingWithGrep() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");

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

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");

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

  @Test
  public void testSampleWithSize() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");
    int sampleSize = 2;

    // sample with replacement
    List<Object[]> randomSample = ddf.VIEWS.getRandomSample(sampleSize, true, 123);
    Assert.assertTrue(randomSample.size() == sampleSize);

    // sample without replacement
    randomSample = ddf.VIEWS.getRandomSample(sampleSize, false, 123);
    Assert.assertTrue(randomSample.size() == sampleSize);

    try {
      randomSample = ddf.VIEWS.getRandomSample(-1, false, 123);
      Assert.fail("Should not be able to oversampling without replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Number of samples must be larger than or equal to 0"));
    }
  }

  @Test
  public void testSampleWithSizeToDDF() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");
    long[] sampleSizes = {0, 2, ddf.getNumRows()};

    for(long sampleSize: sampleSizes) {
      // sample with replacement
      DDF randomSample = ddf.VIEWS.getRandomSampleByNum(sampleSize, true, 123);
      Assert.assertTrue(randomSample.getNumRows() == sampleSize);

      // sample without replacement
      randomSample = ddf.VIEWS.getRandomSampleByNum(sampleSize, false, 123);
      Assert.assertTrue(randomSample.getNumRows() == sampleSize);
    }

    try {
      DDF randomSample = ddf.VIEWS.getRandomSampleByNum(-1, false, 123);
      Assert.fail("Should not be able to oversampling without replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Number of samples must be larger than or equal to 0"));
    }
  }

  @Test
  public void testSampleWithFraction() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");
    long numRows = ddf.getNumRows();

    double[] fractions = {0, 0.5, 1};

    for(double fraction: fractions) {
      // sample with replacement
      DDF ddf2 = ddf.VIEWS.getRandomSample(fraction, true, 123);
      Assert.assertTrue(ddf2.getNumRows() == Math.round(numRows * fraction));

      // sample without replacement
      ddf2 = ddf.VIEWS.getRandomSample(fraction, false, 123);
      Assert.assertTrue(ddf2.getNumRows() == Math.round(numRows * fraction));
    }


    try {
      DDF ddf2 = ddf.VIEWS.getRandomSample(-1.0, false, 123);
      Assert.fail("Sample fraction must be >= 0 in sampling without replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Sampling fraction must be from 0 to 1 in sampling without replacement"));
    }

    try {
      DDF ddf2 = ddf.VIEWS.getRandomSample(-1.0, true, 123);
      Assert.fail("Sample fraction must be >= 0 in sampling with replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Sampling fraction must be larger or equal to 0 in sampling with replacement"));
    }
  }


  @Test
  public void testSampleWithFractionApprox() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");

    // sample with replacement
    // In Spark, size of the sampled DDF is not always equal to size of original DDF * fraction
    DDF ddf2 = ddf.VIEWS.getRandomSampleApprox(0.5, true, 123);
    Assert.assertTrue(ddf2.getNumRows() > 0);

    // sample without replacement
    ddf2 = ddf.VIEWS.getRandomSampleApprox(0.5, false, 123);
    Assert.assertTrue(ddf2.getNumRows() > 0);

    try {
      ddf2 = ddf.VIEWS.getRandomSampleApprox(-1.0, false, 123);
      Assert.fail("Sample fraction must be >= 0 in sampling without replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Sampling fraction must be from 0 to 1 in sampling without replacement"));
    }

    try {
      ddf2 = ddf.VIEWS.getRandomSampleApprox(-1.0, true, 123);
      Assert.fail("Sample fraction must be >= 0 in sampling with replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Sampling fraction must be larger or equal to 0 in sampling with replacement"));
    }
  }

  @Test
  public void testOversampling() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select * from airline", "SparkSQL");
    long numRows = ddf.getNumRows();

    // sample2ddf by size
    DDF ddf2 = ddf.VIEWS.getRandomSampleByNum(numRows * 2, true, 123);
    Assert.assertTrue(ddf2.getNumRows() == numRows * 2);

    // sample2ddf by fraction

    ddf2 = ddf.VIEWS.getRandomSample(2.0, true, 123);

    Assert.assertTrue(ddf2.getNumRows() == numRows * 2);

    ddf2 = ddf.VIEWS.getRandomSampleApprox(2.0, true, 123);

    Assert.assertTrue(ddf2.getNumRows() > numRows);


    try {
      ddf2 = ddf.VIEWS.getRandomSample(2.0, false, 123);
      Assert.fail("Should not be able to oversampling without replacement");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Sampling fraction must be from 0 to 1 in sampling without replacement"));
    }

    // sampling with size > original size with replacement returns size rows
    int sampleSize = (int)ddf.getNumRows() * 2;
    List<Object[]> randomSample = ddf.VIEWS.getRandomSample(sampleSize, true, 123);
    Assert.assertTrue(randomSample.size() == sampleSize);

    // sampling with size > original size without replacement always returns the original rows
    randomSample = ddf.VIEWS.getRandomSample(sampleSize, false, 123);
    Assert.assertEquals(randomSample.size(), ddf.getNumRows());
  }
}
