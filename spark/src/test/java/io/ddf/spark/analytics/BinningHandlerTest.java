package io.ddf.spark.analytics;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import io.ddf.types.AggregateTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class BinningHandlerTest extends BaseTest {

  @Test
  public void testBinningToInteger() throws DDFException {
    createTableAirline();

    DDF ddf = manager
            .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                    "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                    "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    DDF ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, false, true, true);

    Assert.assertEquals(Schema.ColumnType.INT, ddf1.getSchemaHandler().getColumn("dayofweek").getType());

    AggregateTypes.AggregationResult ret = ddf1.xtabs("dayofweek, COUNT(*)");
    Assert.assertEquals(ret.keySet().size(), 1);
    Assert.assertFalse(ret.keySet().contains("1"));
    Assert.assertEquals(ret.get("0")[0], 31, 0);

    try {
      ddf.binning("dayofweek", "EQUALFREQ", 2, null, false, true, true);
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().equalsIgnoreCase("Breaks must be unique: [4.0, 4.0, 4.0]"));
    }

    DDF ddf2 = ddf.binning("arrdelay", "EQUALFREQ", 2, null, false, true, true);
    Assert.assertEquals(Schema.ColumnType.INT, ddf2.getSchemaHandler().getColumn("arrdelay").getType());

    ret = ddf2.xtabs("arrdelay, COUNT(*)");
    Assert.assertEquals(ret.keySet().size(), 2);
    Assert.assertTrue(ret.keySet().containsAll(Arrays.asList("0", "1")));

    DDF ddf3 = ddf.binning("month", "custom", 0, new double[] { 1, 2, 4, 6, 8, 10, 12 }, false, true, true);
    Assert.assertEquals(Schema.ColumnType.INT, ddf2.getSchemaHandler().getColumn("month").getType());

    ret = ddf3.xtabs("month, COUNT(*)");
    Assert.assertEquals(ret.keySet().size(), 6);
    Assert.assertTrue(ret.keySet().containsAll(Arrays.asList("0", "1", "2", "3", "4", "5")));

    ddf3 = ddf.binning("month", "custom", 0, new double[] { 2, 4, 6, 8 }, false, true, true);
    // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    Assert.assertEquals(Schema.ColumnType.INT, ddf2.getSchemaHandler().getColumn("month").getType());

    ret = ddf3.xtabs("month, COUNT(*)");
    Assert.assertEquals(ret.keySet().size(), 4);
    Assert.assertTrue(ret.keySet().containsAll(Arrays.asList("0", "1", "2", "null")));

  }

  @Test
  public void testBinningToLabels() throws DDFException {

    createTableAirline();

    DDF ddf = manager
            .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                    "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                    "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    DDF ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true);

    Assert.assertEquals(ColumnClass.FACTOR, ddf1.getColumn("dayofweek").getColumnClass());
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().isPresent());
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4]", "(4,4.004]")));

    try {
      ddf.binning("dayofweek", "EQUALFREQ", 2, null, true, true, true);
    } catch (DDFException ex) {
      Assert.assertTrue(ex.getMessage().equalsIgnoreCase("Breaks must be unique: [4.0, 4.0, 4.0]"));
    }

    // Binning by equal frequency always uses includeLowest = true
    DDF ddf2 = ddf.binning("arrdelay", "EQUALFREQ", 2, null, true, true, true);
    Assert.assertEquals(ColumnClass.FACTOR, ddf2.getColumn("arrdelay").getColumnClass());
    Assert.assertTrue(ddf2.getColumn("arrdelay").getOptionalFactor().getLevels().isPresent());
    Assert.assertTrue(ddf2.getColumn("arrdelay").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[-24,4]", "(4,80]")));

    ddf2 = ddf.binning("arrdelay", "EQUALFREQ", 2, null, true, false, false);
    Assert.assertEquals(ColumnClass.FACTOR, ddf2.getColumn("arrdelay").getColumnClass());
    Assert.assertTrue(ddf2.getColumn("arrdelay").getOptionalFactor().getLevels().isPresent());
    Assert.assertTrue(ddf2.getColumn("arrdelay").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[-24,4)", "[4,80]")));


    DDF ddf3 = ddf.binning("month", "custom", 0, new double[] { 1, 2, 4, 6, 8, 10, 12 }, true, true, true);
    Assert.assertEquals(ColumnClass.FACTOR, ddf3.getColumn("month").getColumnClass());
    Assert.assertTrue(ddf3.getColumn("month").getOptionalFactor().getLevels().isPresent());
    Assert.assertTrue(ddf3.getColumn("month").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[1,2]", "(2,4]", "(4,6]", "(6,8]", "(10,12]")));

    ddf3 = ddf.binning("month", "custom", 0, new double[] { 2, 4, 6, 8 }, true, true, true);
    // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    Assert.assertEquals(ColumnClass.FACTOR, ddf3.getColumn("month").getColumnClass());
    Assert.assertTrue(ddf3.getColumn("month").getOptionalFactor().getLevels().isPresent());
    Assert.assertTrue(ddf3.getColumn("month").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[2,4]", "(4,6]", "(6,8]")));


  }
  @Test
  public void testBinningToLabelsWithRightIncludeLowest() throws DDFException {
    createTableAirline();

    DDF ddf = manager
            .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                    "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                    "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    DDF ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true);
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4]", "(4,4.004]")));
    Map<String, Integer> levelCounts = ddf1.getSchemaHandler().computeLevelCounts("dayofweek");
    Assert.assertFalse(levelCounts.containsKey("(4,4.004]"));
    Assert.assertTrue(levelCounts.get("[3.996,4]") == 31);

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, false);
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4)", "[4,4.004]")));
    levelCounts = ddf1.getSchemaHandler().computeLevelCounts("dayofweek");
    Assert.assertFalse(levelCounts.containsKey("[3.996,4)"));
    Assert.assertTrue(levelCounts.get("[4,4.004]") == 31);

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, false, false);
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4)", "[4,4.004]")));
    levelCounts = ddf1.getSchemaHandler().computeLevelCounts("dayofweek");
    Assert.assertFalse(levelCounts.containsKey("[3.996,4)"));
    Assert.assertTrue(levelCounts.get("[4,4.004]") == 31);

    ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, false, true);
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4]", "(4,4.004]")));
    levelCounts = ddf1.getSchemaHandler().computeLevelCounts("dayofweek");
    Assert.assertFalse(levelCounts.containsKey("(4,4.004]"));
    Assert.assertTrue(levelCounts.get("[3.996,4]") == 31);

  }

  @Test
  public void testBinningToLabelsPrecision() throws DDFException {
    createTableAirline();

    DDF ddf = manager
            .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                    "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                    "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    DDF ddf1 = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true, true, 1);
    Assert.assertTrue(ddf1.getColumn("dayofweek").getOptionalFactor().getLevels().get().containsAll(Arrays.asList("[3.996,4]", "(4,4.004]")));
    Map<String, Integer> levelCounts = ddf1.getSchemaHandler().computeLevelCounts("dayofweek");
    Assert.assertFalse(levelCounts.containsKey("(4,4.004]"));
    Assert.assertTrue(levelCounts.get("[3.996,4]") == 31);

  }

  @Test
  public void testBinningInvalidCases() throws DDFException {
    createTableAirline();

    DDF ddf = manager
            .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                    "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                    "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    try {
      ddf.binning("month", "custom", 0, new double[]{4, 2, 6, 8}, true, true, true, 1);
      Assert.fail("Should raise Exception on non-monotonical breaks");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().equalsIgnoreCase("Please enter increasing breaks"));
    }
  }

}
