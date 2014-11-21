package io.ddf.analytics;


import io.ddf.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SummaryTest {
  private double[] da, db, dc, dd;
  private Summary a, b, c, d;

  @Before
  public void setUp() throws Exception {
    da = new double[] { 1, 2, 3 };
    db = new double[] { 5, Double.NaN, 7, 9 };
    dc = new double[] { Double.NaN, Double.NaN };
    dd = new double[] { 1, 2, 3, 5, Double.NaN, 7, 9, Double.NaN, Double.NaN };
    a = new Summary(da);
    b = new Summary(db);
    c = new Summary(dc);
    d = new Summary(dd);
  }

  @Test
  public void testMergeDoubleArray() {
    assertArrayEquals(
        "Expected exacted matching",
        new double[] { Utils.roundUp(d.mean()), Utils.roundUp(d.stdev()),
            Utils.roundUp(d.variance()), Utils.roundUp(d.min()),
            Utils.roundUp(d.max()) }, new double[] { 4.5, 3.08, 9.5, 1, 9 },
        0.0
    );
    assertArrayEquals(new long[] { d.count(), d.NACount() },
        new long[] { 6, 3 });
    assertEquals(c.mean(), Double.NaN, 0.0);
  }

  @Test
  public void testMergeSummary() {
    Summary e = a.merge(b).merge(c);
    assertEquals(d.mean(), e.mean(), 0.0);
    assertEquals(d.stdev(), e.stdev(), 0.0);
    assertEquals(a, e);
    assertFalse(d.equals(e));
    assertTrue(a.toString().equals(
        "mean:4.5 stdev:3.08 var:9.5 cNA:3 count:6 min:1.0 max:9.0"));
  }

  @Test
  public void testToString() {
    assertTrue(a.toString().equals(
        "mean:2.0 stdev:1.0 var:1.0 cNA:0 count:3 min:1.0 max:3.0"));
    assertTrue(d.toString().equals(
        "mean:4.5 stdev:3.08 var:9.5 cNA:3 count:6 min:1.0 max:9.0"));
  }
}
