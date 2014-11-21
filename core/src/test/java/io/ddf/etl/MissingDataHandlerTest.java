package io.ddf.etl;


import io.ddf.etl.IHandleMissingData.Axis;
import io.ddf.etl.IHandleMissingData.NAChecking;
import org.junit.Assert;
import org.junit.Test;

public class MissingDataHandlerTest {

  @Test
  public void testEnums() {
    Assert.assertEquals(Axis.COLUMN, Axis.fromString("column"));
    Assert.assertEquals(NAChecking.ALL, NAChecking.fromString("all"));
    Assert.assertTrue(Axis.fromString("column") == Axis.COLUMN);
  }

}
