package io.ddf.analytics;


import static org.junit.Assert.assertTrue;
import org.junit.Assert;
import org.junit.Test;
import io.ddf.types.AggregateTypes.AggregateField;
import io.ddf.facades.RFacade;

public class AggregationTest {

  @Test
  public void testAggregateSql() throws Exception {
    // aggregation: select year, month, min(depdelay), max(arrdelay) from airline group by year, month;
    String fields = "year, month, min(depdelay), max(arrdelay)";
    String expectedSql = "SELECT year,month,MIN(depdelay),MAX(arrdelay) FROM airline GROUP BY year,month";
    Assert.assertEquals(expectedSql, AggregateField.toSql(AggregateField.fromSqlFieldSpecs(fields), "airline"));
  }

  @Test
  public void testRAggregateFormular() {

    String rAggregateFormula = "cbind(mpg,hp) ~ vs + am, mtcars, FUN=mean";
    assertTrue(rAggregateFormula.matches("^\\s*cbind\\((.+)\\)\\s*~\\s*(.+),(.+),(.+)"));

    Assert.assertEquals("vs,am,mean(mpg),mean(hp)", RFacade.parseRAggregateFormula(rAggregateFormula));
  }

  @Test
  public void testAggregateTypes() {
    Assert.assertEquals("COUNT(*) AS m", AggregateField.fromFieldSpec("count(*)").setName("m").toString());
  }
}
