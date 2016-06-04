package io.ddf.spark.etl;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema;
import io.ddf.content.Schema.ColumnType;
import io.ddf.datasource.*;
import io.ddf.etl.TransformationHandler;
import io.ddf.exception.DDFException;
import io.ddf.s3.S3DDF;
import io.ddf.s3.S3DDFManager;
import io.ddf.spark.BaseTest;
import org.junit.*;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class TransformationHandlerTest extends BaseTest {
  private DDF ddf;
  private S3DDFManager s3DDFManager;

  @Before
  public void setUp() throws Exception {
    
    createTableAirline();
    createTableAirlineBigInt();
    ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
            "distance, arrdelay, depdelay from airline", "SparkSQL");

    S3DataSourceDescriptor s3dsd = new S3DataSourceDescriptor(new S3DataSourceURI("ada-demo-data/sleep_data_sample.json"),
            new S3DataSourceCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")),
            null,
            new FileFormat(DataFormat.JSON));
    s3DDFManager = (S3DDFManager) DDFManager.get(DDFManager.EngineType.S3, s3dsd);
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
  public void testTransformNativeRserveSingleExpressionInPlaceTrue() throws DDFException {
    Boolean inPlace = Boolean.TRUE;
    DDF newDdf = ddf.copy();
    DDF newDdf2 = newDdf.Transform.transformNativeRserve("newcol = deptime / arrtime", inPlace);
    List<String> res = newDdf2.VIEWS.head(10);

    Assert.assertNotNull(newDdf);
    Assert.assertNotNull(newDdf2);
    Assert.assertTrue("With inPlace being true, two DDF should have the same UUID", newDdf.getUUID().equals(newDdf2.getUUID()));
    Assert.assertEquals("With inPlace being true, original DDF should have newcol added", "newcol", newDdf.getColumnName(8));
    Assert.assertEquals("transformed DDF newDdf2 should have newcol added", "newcol", newDdf2.getColumnName(8));
    Assert.assertEquals(10, res.size());
  }

  @Test
  public void testTransformNativeRserveSingleExpressionInPlaceFalse() throws DDFException {
    Boolean inPlace = Boolean.FALSE;
    DDF newDdf3 = ddf.copy();
    DDF newDdf4 = newDdf3.Transform.transformNativeRserve("newcol = deptime / arrtime", inPlace);

    Assert.assertNotNull(newDdf3);
    Assert.assertNotNull(newDdf4);
    Assert.assertEquals("transformed DDF newDdf4 should have newcol added", "newcol", newDdf4.getColumnName(8));
    Assert.assertFalse("With inPlace being false, two DDF should have different UUID", newDdf3.getUUID().equals(newDdf4.getUUID()));
  }

  @Test
  public void testTransformNativeRserveMultipleExpressionInPlaceTrue() throws DDFException {
    Boolean inPlace = Boolean.TRUE;
    String[] expressions = {"newcol = deptime / arrtime","newcol2=log(arrdelay)"};
    DDF newDdf = ddf.copy();
    DDF newDdf2 = newDdf.Transform.transformNativeRserve(expressions, inPlace);

    Assert.assertNotNull(newDdf);
    Assert.assertNotNull(newDdf2);
    Assert.assertTrue("With inPlace being true, two DDF should have the same UUID", newDdf.getUUID().equals(newDdf2.getUUID()));
    Assert.assertEquals("With inPlace being true, original DDF should have newcol added", "newcol", newDdf.getColumnName(8));
    Assert.assertEquals("With inPlace being true, original DDF should have newcol2 added", "newcol2", newDdf.getColumnName(9));
    Assert.assertEquals("transformed DDF newDdf2 should have newcol added", "newcol", newDdf2.getColumnName(8));
    Assert.assertEquals("transformed DDF newDdf2 should have newcol2 added", "newcol2", newDdf2.getColumnName(9));
  }

  @Test
  public void testTransformNativeRserveMultipleExpressionInPlaceFalse() throws DDFException {
    Boolean inPlace = Boolean.FALSE;
    String[] expressions = {"newcol = deptime / arrtime","newcol2=log(arrdelay)"};
    DDF newDdf3 = ddf.copy();
    DDF newDdf4 = newDdf3.Transform.transformNativeRserve(expressions, inPlace);

    Assert.assertNotNull(newDdf3);
    Assert.assertNotNull(newDdf4);
    Assert.assertFalse("With inPlace being false, two DDF should have different UUID", newDdf3.getUUID().equals(newDdf4.getUUID()));
    Assert.assertEquals("transformed DDF newDdf4 should have newcol added", "newcol", newDdf4.getColumnName(8));
    Assert.assertEquals("transformed DDF newDdf4 should have newcol2 added", "newcol2", newDdf4.getColumnName(9));
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
    Assert.assertTrue(newDDF.getUUID() != ddf.getUUID());
    Assert.assertTrue(newDDF.getColumn("year").getType() == ColumnType.STRING);
  }

  @Test
  public void testCastTypeMutable() throws DDFException {
    DDF inputDDF = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
            "distance, arrdelay, depdelay from airline", "SparkSQL");

    DDF newDDF = inputDDF.Transform.castType("year", "string", true);
    Assert.assertTrue(newDDF.getUUID().equals(inputDDF.getUUID()));
    Assert.assertTrue(newDDF.getColumn("year").getType() == ColumnType.STRING);
    Assert.assertTrue(inputDDF.getColumn("year").getType() == ColumnType.STRING);
  }

  @Test
  public void testCastTypeMultipleColumns() throws DDFException {
    List<String> columns = new ArrayList<>();
    columns.add("year");
    columns.add("month");

    Assert.assertTrue(ddf.getColumn("year").getType() == ColumnType.INT);
    Assert.assertTrue(ddf.getColumn("month").getType() == ColumnType.INT);

    DDF newDDF = ddf.Transform.castType(columns, "string", Boolean.FALSE);
    Assert.assertFalse(newDDF.getUUID().equals(ddf.getUUID()));
    Assert.assertTrue(newDDF.getColumn("year").getType() == ColumnType.STRING);
    Assert.assertTrue(newDDF.getColumn("month").getType() == ColumnType.STRING);
  }

  @Test
  public void testCastTypeMultipleColumnsInPlaceTrue() throws DDFException {
    DDF inputDDF = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, " +
            "distance, arrdelay, depdelay from airline", "SparkSQL");

    List<String> columns = new ArrayList<>();
    columns.add("year");
    columns.add("month");

    Assert.assertTrue(inputDDF.getColumn("year").getType() == ColumnType.INT);
    Assert.assertTrue(inputDDF.getColumn("month").getType() == ColumnType.INT);

    Boolean inPlace = Boolean.TRUE;

    DDF newDDF = inputDDF.Transform.castType(columns, "string", inPlace);
    Assert.assertTrue(newDDF.getUUID().equals(inputDDF.getUUID()));
    Assert.assertTrue(newDDF.getColumn("year").getType() == ColumnType.STRING);
    Assert.assertTrue(newDDF.getColumn("month").getType() == ColumnType.STRING);
    Assert.assertTrue(inputDDF.getColumn("year").getType() == ColumnType.STRING);
    Assert.assertTrue(inputDDF.getColumn("month").getType() == ColumnType.STRING);
  }

  @Test
  public void testFlattenDDFAllColumns() throws Exception {
    S3DDF allJsonDDF = s3DDFManager.newDDF("ada-demo-data", "sleep_data_sample_test.json", null, null);
    DDF originalDDF = manager.copyFrom(allJsonDDF);
    DDF flattenedDDF = originalDDF.Transform.flattenDDF();

    Assert.assertNotNull(flattenedDDF);
    Assert.assertTrue(originalDDF.getColumnNames().size() == 8);
    Assert.assertTrue(flattenedDDF.getColumnNames().size() == 15);
    Assert.assertTrue(flattenedDDF.getNumRows() == 100);

    Assert.assertTrue(flattenedDDF.getColumnNames().contains("created_at_date"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("uid_oid"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("u_at_date"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("id_oid"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_bookmarkTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realEndTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_isFirstSleepOfDay"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_normalizedSleepQuality"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realDeepSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realStartTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_sleepStateChanges"));
  }

  @Test
  public void testFlattenDDFSingleColumn() throws Exception {
    S3DDF allJsonDDF = s3DDFManager.newDDF("ada-demo-data", "sleep_data_sample_test.json", null, null);
    DDF originalDDF = manager.copyFrom(allJsonDDF);
    DDF flattenedDDF = originalDDF.Transform.flattenDDF(new String[]{"data"});

    Assert.assertNotNull(flattenedDDF);
    Assert.assertTrue(originalDDF.getColumnNames().size() == 8);
    Assert.assertTrue(flattenedDDF.getColumnNames().size() == 8);
    Assert.assertTrue(flattenedDDF.getNumRows() == 100);

    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_bookmarkTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realEndTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_isFirstSleepOfDay"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_normalizedSleepQuality"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realDeepSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realStartTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_sleepStateChanges"));
  }

  @Test
  public void testFlattenDDFInPlaceFalse() throws Exception {
    Boolean inPlace = Boolean.FALSE;
    S3DDF allJsonDDF = s3DDFManager.newDDF("ada-demo-data", "sleep_data_sample_test.json", null, null);
    DDF originalDDF = manager.copyFrom(allJsonDDF);
    DDF flattenedDDF = originalDDF.Transform.flattenDDF(new String[]{"data"}, inPlace);

    Assert.assertNotNull(originalDDF);
    Assert.assertNotNull(flattenedDDF);
    Assert.assertNotSame("inPlace FALSE, two DDF should have different UUID", originalDDF.getUUID(), flattenedDDF.getUUID());

    Assert.assertTrue(flattenedDDF.getColumnNames().size() == 8);
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_bookmarkTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realEndTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_isFirstSleepOfDay"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_normalizedSleepQuality"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realDeepSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realStartTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_sleepStateChanges"));
  }

  @Test
  public void testFlattenDDFInPlaceTrue() throws Exception {
    Boolean inPlace = Boolean.TRUE;
    S3DDF allJsonDDF = s3DDFManager.newDDF("ada-demo-data", "sleep_data_sample_test.json", null, null);
    DDF originalDDF = manager.copyFrom(allJsonDDF);
    DDF flattenedDDF = originalDDF.Transform.flattenDDF(new String[]{"data"}, inPlace);

    Assert.assertNotNull(originalDDF);
    Assert.assertNotNull(flattenedDDF);
    Assert.assertEquals("inPlace TRUE, two DDF should have same UUID", originalDDF.getUUID(), flattenedDDF.getUUID());

    Assert.assertTrue(flattenedDDF.getColumnNames().size() == 8);
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_bookmarkTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realEndTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_isFirstSleepOfDay"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_normalizedSleepQuality"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realDeepSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realSleepTimeInMinutes"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_realStartTime"));
    Assert.assertTrue(flattenedDDF.getColumnNames().contains("data_sleepStateChanges"));
  }
}
