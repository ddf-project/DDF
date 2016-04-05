package io.ddf.spark.etl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.ddf.DDF;
import io.ddf.etl.Types.JoinType;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinHandlerTest extends BaseTest {
  private DDF left_ddf, right_ddf;
  
  @Before
  public void setUp() throws Exception {
    createTableMtcars();
    createTableCarOwner();

    left_ddf = manager.sql2ddf("select * from mtcars", "SparkSQL");
    right_ddf = manager.sql2ddf("select * from carowner", "SparkSQL");
  }
  
  @Test
  public void testInnerJoin() throws DDFException {
    DDF ddf = left_ddf.join(right_ddf, JoinType.INNER, Arrays.asList("cyl"),null,null, null, null);
    LOG.info("Column names: " +ddf.getColumnNames());
    Assert.assertEquals(25, ddf.getNumRows());
  }

  @Test
  public void testSuffix() throws DDFException {
    DDF ddf = left_ddf.join(right_ddf, JoinType.INNER, Arrays.asList("cyl"),null,null, "_left", "_right");
    List<String> colNames = ddf.getColumnNames();
    Assert.assertTrue("expect to have cyl column with left suffix", colNames.contains("cyl_left"));
    Assert.assertTrue("expect to have cyl column with right suffix", colNames.contains("cyl_right"));
    Assert.assertTrue("expect to have disp column with left suffix", colNames.contains("disp_left"));
    Assert.assertTrue("expect to have disp column with right suffix", colNames.contains("disp_right"));
    Assert.assertFalse("expect to column name to not have suffix", colNames.contains("name_right"));
  }

  @Test
  public void testGenerateSelectColumns() throws DDFException {
    String columnId = "lt";
    String suffix = "_l";

    List<String> targetColumns = new ArrayList<>();
    targetColumns.add("col1");
    targetColumns.add("col2");
    targetColumns.add("col3");
    List<String> filterColumns = new ArrayList<>();
    filterColumns.add("col2");
    filterColumns.add("col3");
    filterColumns.add("col4");

    String selectColumn = ((JoinHandler)left_ddf.getJoinsHandler()).generateSelectColumns(targetColumns, filterColumns, columnId, suffix);
    Assert.assertEquals("Test1: Select column string is supposed to match", "lt.col1,lt.col2 AS col2_l,lt.col3 AS col3_l", selectColumn);

    targetColumns.clear();
    targetColumns.add("col1");
    targetColumns.add("col2");
    targetColumns.add("col3");
    filterColumns.clear();

    selectColumn = ((JoinHandler)left_ddf.getJoinsHandler()).generateSelectColumns(targetColumns, filterColumns, columnId, suffix);
    Assert.assertEquals("Test2: Select column string is supposed to match", "lt.col1,lt.col2,lt.col3", selectColumn);

    targetColumns.clear();

    selectColumn = ((JoinHandler)left_ddf.getJoinsHandler()).generateSelectColumns(targetColumns, filterColumns, columnId, suffix);
    Assert.assertEquals("Test3: Select column string is supposed to be empty", "", selectColumn);

    selectColumn = ((JoinHandler)left_ddf.getJoinsHandler()).generateSelectColumns(null, filterColumns, columnId, suffix);
    Assert.assertEquals("Test4: Select column string is supposed to be empty", "", selectColumn);

    targetColumns.clear();
    targetColumns.add("col1");
    targetColumns.add("col2");
    targetColumns.add("col3");
    selectColumn = ((JoinHandler)left_ddf.getJoinsHandler()).generateSelectColumns(targetColumns, null, columnId, suffix);
    Assert.assertEquals("Test5: Select column string is supposed to match", "lt.col1,lt.col2,lt.col3", selectColumn);
  }
  
}
