package io.ddf.etl;


import org.junit.Assert;
import org.junit.Test;
import io.ddf.etl.IHandleMissingData.Axis;
import io.ddf.etl.IHandleMissingData.NAChecking;

public class MissingDataHandlerTest {

  @Test
  public void testEnums() {
    Assert.assertEquals(Axis.COLUMN, Axis.fromString("column"));
    Assert.assertEquals(NAChecking.ALL, NAChecking.fromString("all"));
    Assert.assertTrue(Axis.fromString("column") == Axis.COLUMN);
  }

}
