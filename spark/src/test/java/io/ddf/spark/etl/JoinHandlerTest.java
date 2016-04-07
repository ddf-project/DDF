package io.ddf.spark.etl;

import java.util.Arrays;
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
    DDF ddf = left_ddf.join(right_ddf, JoinType.INNER, Arrays.asList("cyl"),null,null);
    LOG.info("Column names: " +ddf.getColumnNames());
    Assert.assertEquals(25, ddf.getNumRows());
  }
  
}
