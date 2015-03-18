package io.spark.ddf.etl;


import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.Summary;
import io.ddf.content.Schema.ColumnType;
import io.ddf.etl.TransformationHandler;
import io.ddf.exception.DDFException;
import io.spark.ddf.BaseTest;
import org.junit.*;
import java.util.List;

public class TransformationHandlerTest extends BaseTest {
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    createTableAirline();
    ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
  }

  @Test
  public void testTransformNativeRserve() throws DDFException {
    DDF newddf = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime");
    System.out.println("name " + ddf.getName());
    System.out.println("newname " + newddf.getName());
    List<String> res = ddf.VIEWS.head(10);

    Assert.assertFalse(ddf.getName().equals(newddf.getName()));
    Assert.assertNotNull(newddf);
    Assert.assertEquals("newcol", newddf.getColumnName(8));
    Assert.assertEquals(10, res.size());
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

  @Test
  public void testTransformMapReduceNative() throws DDFException {
    // aggregate sum of month group by year

    String mapFuncDef = "function(part) { keyval(key=part$year, val=part$month) }";
    String reduceFuncDef = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }";
    DDF newddf = ddf.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef);
    System.out.println("name " + ddf.getName());
    System.out.println("newname " + newddf.getName());
    Assert.assertNotNull(newddf);
    Assert.assertTrue(newddf.getColumnName(0).equals("key"));
    Assert.assertTrue(newddf.getColumnName(1).equals("val"));

    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(0).getType() == ColumnType.STRING);
    Assert.assertTrue(newddf.getSchemaHandler().getColumns().get(1).getType() == ColumnType.INT);
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
    List<String> cols = Lists.newArrayList("distance", "arrtime", "deptime");

    ddf = ddf.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf.getNumRows());
    Assert.assertEquals(4, ddf.getNumColumns());
    Assert.assertEquals("speed", ddf.getColumnName(3));

    ddf.setMutable(false);

    // multiple expressions, column name with special characters
    DDF ddf3 = ddf.Transform.transformUDF("arrtime-deptime, (speed^*- = distance/(arrtime-deptime)", cols);
    Assert.assertEquals(31, ddf3.getNumRows());
    Assert.assertEquals(5, ddf3.getNumColumns());
    Assert.assertEquals("speed", ddf3.getColumnName(4));
    Assert.assertEquals(5, ddf3.getSummary().length);
    
    // transform using if else/case when
    
    List<String> lcols = Lists.newArrayList("distance", "arrtime", "deptime", "arrdelay");
    String s0 = "new_col = if(arrdelay=15,1,0)";
    String s1 = "new_col = if(arrdelay=15,1,0),v ~ (arrtime-deptime),distance/(arrtime-deptime)";
    String s2 = "arr_delayed=if(arrdelay=\"yes\",1,0)";
    String s3 = "origin_sfo = case origin when \'SFO\' then 1 else 0 end ";
    Assert.assertEquals("(if(arrdelay=15,1,0)) as new_col,((arrtime-deptime)) as v,(distance/(arrtime-deptime))",
        TransformationHandler.RToSqlUdf(s1));
    Assert.assertEquals("(if(arrdelay=\"yes\",1,0)) as arr_delayed", TransformationHandler.RToSqlUdf(s2));
    Assert.assertEquals("(case origin when \'SFO\' then 1 else 0 end) as origin_sfo", TransformationHandler.RToSqlUdf(s3));
    
    DDF ddf2 = ddf.Transform.transformUDF(s1, lcols);
    Assert.assertEquals(31, ddf2.getNumRows());
    Assert.assertEquals(7, ddf2.getNumColumns());
  }
}
