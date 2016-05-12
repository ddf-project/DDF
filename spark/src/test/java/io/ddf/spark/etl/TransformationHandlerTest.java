package io.ddf.spark.etl;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema;
import io.ddf.content.Schema.ColumnType;
import io.ddf.etl.TransformationHandler;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class TransformationHandlerTest extends BaseTest {
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    
    createTableAirline();
    createTableAirlineBigInt();
    ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
            "distance, arrdelay, depdelay from airline", "SparkSQL");
  }

  @Test
  public void testTransformNativeRserve() throws DDFException {
    DDF newddf = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime");
    LOG.info("name " + ddf.getName());
    LOG.info("newname " + newddf.getName());
    List<String> res = ddf.VIEWS.head(10);

    // Assert.assertFalse(ddf.getName().equals(newddf.getName()));
    Assert.assertNotNull(newddf);
    Assert.assertEquals("newcol", newddf.getColumnName(8));
    Assert.assertEquals(10, res.size());
  }

  @Test
  public void testTransformPython() throws DDFException {
    // f = lambda x: x/2.
    // f = lambda x: x+10
    DDF newDdf = ddf.Transform.transformPython(
        new String[] {"ZGVmIHRyYW5zKHgpOgogIHJldHVybiB4LzIuCg==", "ZGVmIHRyYW5zMih4KToKICByZXR1cm4geCsxMAo="},
        new String[] {"trans", "trans2"},
        new String[] {null, "col2"},
        new String[][] { new String[] {"distance"}, new String[] {"month"}});

    Assert.assertFalse(ddf.getUUID().equals(newDdf.getUUID()));
    Assert.assertNotNull(newDdf);
    Assert.assertTrue(newDdf.getColumnNames().contains("c0"));
    Assert.assertTrue(newDdf.getColumnNames().contains("col2"));
    Assert.assertEquals(10, newDdf.getNumColumns());

    try {
      newDdf.Transform.transformPython(
          new String[] {"ZGVmIHRyYW5zKHgpOgogIHJldHVybiB4LzIuCg==", "ZGVmIHRyYW5zMih4KToKICByZXR1cm4geCsxMAo="},
          new String[] {"trans", "TrAnS2"}, new String[] {null, "col2"},
          new String[][] {new String[] {"distance"}, new String[] {"month"}});
      Assert.fail("Should throw error for duplicated column");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }
  }

  @Test
  public void testTransformPythonInPlace() throws DDFException {
    // f = lambda x: x/2.
    // f = lambda x: x+10
    DDF newDdf = ddf.copy();
    DDF newDdf2 = newDdf.Transform.transformPython(
        new String[] {"ZGVmIHRyYW5zKHgpOgogIHJldHVybiB4LzIuCg==", "ZGVmIHRyYW5zMih4KToKICByZXR1cm4geCsxMAo="},
        new String[] {"trans", "trans2"},
        new String[] {null, "col2"},
        new String[][] { new String[] {"distance"}, new String[] {"month"}},
        Boolean.TRUE);

    Assert.assertTrue(newDdf.getUUID().equals(newDdf2.getUUID()));
    Assert.assertNotNull(newDdf2);
    Assert.assertTrue(newDdf2.getColumnNames().contains("c0"));
    Assert.assertTrue(newDdf2.getColumnNames().contains("col2"));
    Assert.assertEquals(10, newDdf2.getNumColumns());

    try {
      newDdf.Transform.transformPython(
          new String[] {"ZGVmIHRyYW5zKHgpOgogIHJldHVybiB4LzIuCg==", "ZGVmIHRyYW5zMih4KToKICByZXR1cm4geCsxMAo="},
          new String[] {"trans", "TrAnS2"}, new String[] {null, "col2"},
          new String[][] {new String[] {"distance"}, new String[] {"month"}}, true);
      Assert.fail("Should throw error for duplicated column");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }
  }

  @Test
  public void testTransformNativeRserveMultipleExpressions() throws DDFException {
    String[] expressions = {"newcol = deptime / arrtime","newcol2=log(arrdelay)"};
    DDF newddf = ddf.Transform.transformNativeRserve(expressions);

    Assert.assertEquals("newcol", newddf.getColumnName(8));
    Assert.assertEquals("newcol2", newddf.getColumnName(9));
  }

  @Test
  public void testTransformNativeRserveBigIntSupport() throws DDFException {
    DDF ddf = manager.sql2ddf("select year, month, dayofweek, uniquecarrier, deptime, arrtime, " +
            "distance, arrdelay, depdelay from airline_bigint", "SparkSQL");
    String[] expressions = {"newcol = deptime / arrtime","depdelay=log(depdelay)"};
    DDF newddf = ddf.Transform.transformNativeRserve(expressions);

    Assert.assertEquals(newddf.getColumn("newcol").getType(), ColumnType.DOUBLE);

    Assert.assertEquals(newddf.getColumn("depdelay").getType(), ColumnType.DOUBLE);

    // Existing columns should preserve their types
    Assert.assertEquals(newddf.getColumn("year").getType(), ColumnType.INT);
    Assert.assertEquals(newddf.getColumn("uniquecarrier").getType(), ColumnType.STRING);
    Assert.assertEquals(newddf.getColumn("arrdelay").getType(), ColumnType.BIGINT);
  }

  @Test
  public void testTransformScaleMinMax() throws DDFException {
    DDF newddf0 = ddf.Transform.transformScaleMinMax();
    Summary[] summaryArr = newddf0.getSummary();
    Assert.assertTrue(summaryArr[0].min() < 1);
    Assert.assertTrue(summaryArr[0].max() == 1);

  }

  @Test
  public void testTransformScaleStandard() throws DDFException {
    DDF newddf1 = ddf.Transform.transformScaleStandard();
    Assert.assertEquals(31, newddf1.getNumRows());
    Assert.assertEquals(8, newddf1.getSummary().length);
  }

  @Ignore
  public void testTransformMapReduceNative() throws DDFException {
    // aggregate sum of month group by year

    String mapFuncDef = "function(part) { keyval(key=part$year, val=part$month) }";
    String reduceFuncDef = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }";
    DDF newddf = ddf.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef);
    LOG.info("name " + ddf.getName());
    LOG.info("newname " + newddf.getName());
    Assert.assertNotNull(newddf);
    Assert.assertTrue(newddf.getColumnName(0).equals("key"));
    Assert.assertTrue(newddf.getColumnName(1).equals("val"));

    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(0).getType() == ColumnType.STRING);
    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(1).getType() == ColumnType.INT);
  }

  @Test
  public void testReservedFactor() throws DDFException {
    ddf.setAsFactor("year");
    ddf.setAsFactor("month");

    Assert.assertTrue(ddf.getSchema() != null);
    ddf.getSchemaHandler().computeLevelCounts(new String[]{"year", "month"});

    LOG.info(">>>>> column class = " + ddf.getColumn("year").getColumnClass());
    LOG.info(">>>>> column class = " + ddf.getColumn("month").getColumnClass());

    Assert.assertTrue(ddf.getColumn("year").getColumnClass() == Schema.ColumnClass.FACTOR);
    Assert.assertTrue(ddf.getColumn("month").getColumnClass() == Schema.ColumnClass.FACTOR);

    ddf.setMutable(true);
    ddf = ddf.Transform.transformUDF("test123= round(distance/2, 2)");

    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(9, ddf.getNumColumns());
    Assert.assertEquals("test123", ddf.getColumnName(8));
    Assert.assertEquals(9, ddf.VIEWS.head(1).get(0).split("\\t").length);


    LOG.info(">>>>> column class = " + ddf.getColumn("year").getColumnClass());
    LOG.info(">>>>> column class = " + ddf.getColumn("month").getColumnClass());

    Assert.assertTrue(ddf.getColumn("year").getColumnClass() == Schema.ColumnClass.FACTOR);
    Assert.assertTrue(ddf.getColumn("month").getColumnClass() == Schema.ColumnClass.FACTOR);

    Assert.assertTrue(ddf.getColumn("year").getOptionalFactor() != null);
    Assert.assertTrue(ddf.getColumn("month").getOptionalFactor() != null);
    LOG.info(">>>>>>>>>>>>> " + ddf.getSchema().getColumns());
  }

  @Test
  public void testTransformSql() throws DDFException {


    ddf.setMutable(true);
    ddf = ddf.Transform.transformUDF("dist= round(distance/2, 2)");

    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(9, ddf.getNumColumns());
    Assert.assertEquals("dist", ddf.getColumnName(8));
    Assert.assertEquals(9, ddf.VIEWS.head(1).get(0).split("\\t").length);

    // udf without assigning column name
    ddf.Transform.transformUDF("arrtime-deptime");
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(10, ddf.getNumColumns());
    Assert.assertEquals(10, ddf.getSummary().length);

    // specifying selected column list
    List<String> cols = Lists.newArrayList("distance", "arrtime", "deptime", "arrdelay");

    ddf = ddf.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(5, ddf.getNumColumns());
    Assert.assertEquals("speed", ddf.getColumnName(4));
    ddf.setMutable(false);

    // multiple expressions, column name with special characters
    List<String> expressions = new ArrayList<String>();
    expressions.add("arrtime-deptime");
    expressions.add("(speed^*- = distance/(arrtime-deptime)");

    DDF ddf3 = ddf.Transform.transformUDF(expressions, cols);
    Assert.assertEquals(31, ddf3.getNumRows());
    Assert.assertEquals(6, ddf3.getNumColumns());
    Assert.assertEquals("speed", ddf3.getColumnName(5));
    Assert.assertEquals(6, ddf3.getSummary().length);

    // transform using if else/case when

    List<String> lcols = Lists.newArrayList("distance", "arrtime", "deptime");
    String s0 = "new_col = if(arrdelay=15,1,0)";
    List<String> s1 = new ArrayList<String>();
    s1.add("new_col = if(arrdelay=15,1,0)");
    s1.add("v ~ (arrtime-deptime)");
    s1.add("distance/(arrtime-deptime)");
    String s2 = "arr_delayed=if(arrdelay=\"yes\",1,0)";
    String s3 = "origin_sfo = case origin when \'SFO\' then 1 else 0 end ";
    LOG.info(">>> TransformationHandler.RToSqlUdf(s1) = " + TransformationHandler.RToSqlUdf(s1));
    // >>> TransformationHandler.RToSqlUdf(s1) = if(arrdelay=15,1,0) as new_col,(arrtime-deptime) as
    // v,distance/(arrtime-deptime)");
    Assert.assertEquals("if(arrdelay=15,1,0) as new_col,(arrtime-deptime) as v,distance/(arrtime-deptime)",
        TransformationHandler.RToSqlUdf(s1));
    Assert.assertEquals("if(arrdelay=\"yes\",1,0) as arr_delayed", TransformationHandler.RToSqlUdf(s2));
    LOG.info(">>> TransformationHandler.RToSqlUdf(s3): " + TransformationHandler.RToSqlUdf(s3));
    Assert
        .assertEquals("case origin when \'SFO\' then 1 else 0 end as origin_sfo", TransformationHandler.RToSqlUdf(s3));
    DDF ddf2 = ddf.Transform.transformUDF(s1, lcols);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(6, ddf2.getNumColumns());

    Assert.assertEquals("regexp_extract('yyyy/mm/dd', '.*/([^/])+', 1) as dt",
        TransformationHandler.RToSqlUdf("dt=regexp_extract('yyyy/mm/dd', '.*/([^/])+', 1)"));

    Assert.assertEquals("regexp_extract('hh:mm:ss', '([^:])+:.*', 1) as dt",
        TransformationHandler.RToSqlUdf("dt=regexp_extract('hh:mm:ss', '([^:])+:.*', 1)"));

    Assert.assertEquals("regexp_extract('hh:mm:ss', '.*\\\\+([^+])+', 1) as dt",
        TransformationHandler.RToSqlUdf("dt=regexp_extract('hh:mm:ss', '.*\\\\+([^+])+', 1)"));
  }

  @Test
  public void  testTransformSqlWithNames() throws DDFException {
    ddf.setMutable(false);

    DDF ddf2 = ddf.Transform.transformUDFWithNames(new String[] {"dist"},
        new String[] {"round(distance/2, 2)"}, null);
    Assert.assertEquals(ddf.getNumColumns() + 1, ddf2.getNumColumns());
    Assert.assertNotSame(ddf.getUUID(), ddf2.getUUID());
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(9, ddf2.getNumColumns());
    Assert.assertEquals("dist", ddf2.getColumnName(8));
    Assert.assertEquals(9, ddf2.VIEWS.head(1).get(0).split("\\t").length);

    ddf2 = ddf.Transform.transformUDFWithNames(new String[] {null}, new String[] {"arrtime-deptime"}, null);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(ddf.getNumColumns() + 1, ddf2.getNumColumns());
    Assert.assertEquals(9, ddf2.getNumColumns());
    Assert.assertEquals(9, ddf2.getSummary().length);

    // mixed transform with duplicated name
    ddf2 = ddf.Transform.transformUDFWithNames(new String[] {null, "c0"},
        new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(10, ddf2.getNumColumns());
    Assert.assertEquals(10, ddf2.getSummary().length);

    try {
      ddf.Transform.transformUDFWithNames(new String[] {null}, new String[] {"arrtime-deptime"},
          new String[]{"notexistedColumn"});
      Assert.fail("Should throw exception when selecting non-existing columns");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Selected column 'notexistedColumn' does not exist"));
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"c100", "c100"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name: c100"));
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"newcolumnname", "newColumnName"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"ArrTime", "newColumnName"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }
  }

  @Test
  public void  testTransformSqlWithNamesMutable() throws DDFException {

    DDF ddf2 = ddf.copy();

    DDF ddf3 = ddf2.Transform
        .transformUDFWithNames(new String[] { "dist" }, new String[] { "round(distance/2, 2)" }, null, true);
    Assert.assertEquals(ddf2, ddf3);
    Assert.assertEquals(ddf2.getNumColumns(), ddf3.getNumColumns());
    Assert.assertEquals(31, ddf3.getNumRows());
    Assert.assertEquals(9, ddf3.getNumColumns());
    Assert.assertEquals("dist", ddf3.getColumnName(8));
    Assert.assertEquals(9, ddf3.VIEWS.head(1).get(0).split("\\t").length);

    ddf2.Transform.transformUDFWithNames(new String[] { null }, new String[] { "arrtime-deptime" }, null, true);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(10, ddf2.getNumColumns());
    Assert.assertEquals(10, ddf2.getSummary().length);

    try {
      ddf2.Transform.transformUDFWithNames(new String[] { null, "c0" },
          new String[] { "round(distance/2, 2)", "arrtime-deptime" }, null, true);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }

    // mixed transform with duplicated name
    ddf2.Transform.transformUDFWithNames(new String[] { null, "c1" },
        new String[] { "round(distance/2, 2)", "arrtime-deptime" }, null, true);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(12, ddf2.getNumColumns());
    Assert.assertEquals(12, ddf2.getSummary().length);

    try {
      ddf2.Transform.transformUDFWithNames(new String[] { null }, new String[] { "arrtime-deptime" },
          new String[] { "notexistedColumn" }, true);
      Assert.fail("Should throw exception when selecting non-existing columns");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Selected column 'notexistedColumn' does not exist"));
    }

    try {
      ddf2.Transform.transformUDFWithNames(new String[] {"c100", "c100"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null, true);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name: c100"));
    }

    try {
      ddf2.Transform.transformUDFWithNames(new String[] {"newcolumnname", "newColumnName"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null, true);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }

    try {
      ddf2.Transform.transformUDFWithNames(new String[] {"ArrTime", "newColumnName"},
          new String[] {"round(distance/2, 2)", "arrtime-deptime"}, null, true);
      Assert.fail("Should throw exception when transforming with duplicated column name");
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().contains("Duplicated column name"));
    }
  }

  public void testTransformSqlWithNamesForProperErrorMessages() throws DDFException {
    ddf.setMutable(false);

    // Invalid expression
    try {
      ddf.Transform.transformUDFWithNames(new String[] {"dist"}, new String[] {"=round(distance/2, 2)"}, null);
      Assert.fail("Should throw exception");
    } catch (DDFException ex) {
      Assert.assertEquals(ex.getMessage(), "Column or Expression with invalid syntax: '=round(distance/2, 2)'");
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"dist"}, new String[] {"distance@"}, null);
      Assert.fail("Should throw exception");
    } catch (DDFException ex) {
      Assert.assertEquals(ex.getMessage(), "Expressions or columns containing invalid character @: distance@");
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"dist"}, new String[] {"distance @"}, null);
      Assert.fail("Should throw exception");
    } catch (DDFException ex) {
      Assert.assertEquals(ex.getMessage(), "Column or Expression with invalid syntax: 'distance @'");
    }

    try {
      ddf.Transform.transformUDFWithNames(new String[] {"dist"}, new String[] {"@distance"}, null);
      Assert.fail("Should throw exception");
    } catch (DDFException ex) {
      Assert.assertEquals(ex.getMessage(), "Expressions or columns containing invalid character @: @distance");
    }

    // Invalid new column name
    try {
      ddf.Transform.transformUDFWithNames(new String[] {"_dist"}, new String[] {"round(distance/2, 2)"}, null);
      Assert.fail("Should throw exception");
    } catch (DDFException ex) {
      Assert.assertEquals(ex.getMessage(), "Column or Expression with invalid syntax: '_dist'");

    }
  }

  @Test(expected = RuntimeException.class)
  public void testConflictingColumnDefinitions() throws DDFException {
    ddf.setMutable(true);
    List<String> cols = Lists.newArrayList("distance");
    ddf.Transform.transformUDF("distance=100", cols);
  }

  @Test(expected = RuntimeException.class)
  public void testConflictingColumnDefinitions2() throws DDFException {
    ddf.setMutable(true);
    List<String> s = new ArrayList<String>();
    s.add("distance=100");
    s.add("distance");
    ddf.Transform.transformUDF(s);
  }

  @Test
  public void testMutableDDFBug() throws DDFException {
    ddf.setMutable(true);
    manager.setDDFName(ddf, "ddf_testMutableDDFBug");
    ddf.Transform.transformUDF("col1 = (arrtime - deptime)");
    ddf.Transform.transformUDF("col2 = (arrtime - arrdelay)");
  }

  @Test
  public void testCastType() throws DDFException {
    DDF newDDF = ddf.Transform.castType("year", "string");
    assert(newDDF.getUUID() != ddf.getUUID());
    assert(newDDF.getColumn("year").getType() == ColumnType.STRING);
    assert(ddf.getColumn("year").getType() == ColumnType.STRING);
  }

  @Test
  public void testCastTypeMutable() throws DDFException {
    DDF newDDF = ddf.Transform.castType("year", "string", true);
    assert(newDDF.getUUID() == ddf.getUUID());
    assert(newDDF.getColumn("year").getType() == ColumnType.STRING);
  }
}
