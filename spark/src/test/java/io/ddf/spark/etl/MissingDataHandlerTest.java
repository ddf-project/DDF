package io.ddf.spark.etl;


import io.ddf.DDF;
import io.ddf.etl.IHandleMissingData.Axis;
import io.ddf.etl.IHandleMissingData.NAChecking;
import io.ddf.exception.DDFException;
import io.ddf.types.AggregateTypes.AggregateFunction;
import io.ddf.spark.BaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.lang.AssertionError;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MissingDataHandlerTest extends BaseTest {
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    createTableAirlineWithNA();

    ddf = manager.sql2ddf("select * from airline", false);
  }

  @Test
  public void testDropNA() throws DDFException {
    DDF newddfDropRow = ddf.dropNA();
    DDF newddfDropColumn = ddf.dropNA(Axis.COLUMN);

    Assert.assertEquals(9, newddfDropRow.getNumRows());
    Assert.assertEquals(22, newddfDropColumn.getNumColumns());

    Assert.assertEquals(29, ddf.getMissingDataHandler().dropNA(Axis.COLUMN, NAChecking.ALL, 0, null).getNumColumns());
  }

  @Test
  public void testFillNA() throws DDFException {
    DDF ddf1 = ddf.VIEWS.project(Arrays.asList("year", "origin", "securitydelay", "lateaircraftdelay"));

    // test fill by scalar value
    DDF newddf = ddf1.fillNA("0");
    Assert.assertEquals(282, newddf.aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);
    Assert.assertEquals(301, ddf1.fillNA("1").aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);

    // test fill by aggregate function
    ddf1.getMissingDataHandler().fillNA(null, null, 0, AggregateFunction.MEAN, null, null);

    // test fill by dictionary, with mutable DDF
    Map<String, String> dict = new HashMap<String, String>() {
      {
        put("year", "2000");
        put("securitydelay", "0");
        put("lateaircraftdelay", "1");
      }
    };

    //ddf1.setMutable(true);
    DDF ddf2 = ddf1.getMissingDataHandler().fillNA(null, null, 0, null, dict, null);
    Assert.assertEquals(301, ddf2.aggregate("year, sum(LateAircraftDelay)").get("2008")[0], 0.1);
  }
}
