package io.ddf.spark.analytics;


import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.content.Schema.Column;
import io.ddf.content.Schema.ColumnClass;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import com.google.common.base.Strings;

public class BinningHandlerTest extends BaseTest {

  @Test
  public void testBinning() throws DDFException {

    createTableAirline();

    DDF ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, " +
                "distance, arrdelay, depdelay, carrierdelay, weatherdelay, " +
                "nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    DDF newddf = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true);

    Assert.assertEquals(ColumnClass.FACTOR, newddf.getSchemaHandler().getColumn("dayofweek").getColumnClass());

    Assert.assertEquals(2, newddf.getSchemaHandler().getColumn("dayofweek").getOptionalFactor().getLevels().get().size());

    DDF newddf2 = ddf.binning("dayofweek", "EQUAlFREQ", 2, null, true, true);

    Assert.assertEquals(ColumnClass.FACTOR, newddf2.getSchemaHandler().getColumn("dayofweek").getColumnClass());

    Assert.assertEquals(2, newddf2.getSchemaHandler().getColumn("dayofweek").getOptionalFactor().getLevels().get().size());


    DDF ddf1 = ddf.binning("month", "custom", 0, new double[] { 2, 4, 6, 8 }, true, true);
    Assert.assertTrue(ddf1.getSchemaHandler().getColumn("month").getColumnClass() == ColumnClass.FACTOR);
    // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    Map<String, Map<String, Integer>> map = ddf1.getSchemaHandler().computeLevelCounts(new String[]{"month"});
    Assert.assertTrue(map.get("month").get("[2,4]") > 0);
    Assert.assertEquals(map.get("month").get("[2,4]"), 6, 0);
    Assert.assertFalse(Strings.isNullOrEmpty(newddf.sql("select dayofweek from @this", "").getRows().get(0)));
    Assert.assertFalse(Strings.isNullOrEmpty(ddf1.sql("select month from @this", "").getRows().get(0)));

    Column col = ddf1.getSchemaHandler().getColumn("month");
    MetaInfo mi = new MetaInfo(col.getName(), col.getType().toString().toLowerCase());
    mi = mi.setFactor(col.getOptionalFactor().getLevelMap());
    Assert.assertTrue(mi.hasFactor());

    MetaInfo[] m = generateMetaInfo(newddf.getSchema());
    for (int i = 0; i < m.length; i++) {
      if (m[i].getHeader().equals("dayofweek")) {
        Assert.assertTrue(m[i].hasFactor());
        Assert.assertEquals(2, m[i].getFactor().size());
      }
    }
  }

  public static MetaInfo[] generateMetaInfo(Schema schema) throws DDFException {
    List<Column> columns = schema.getColumns();
    MetaInfo[] metaInfo = new MetaInfo[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
      if (columns.get(i).getColumnClass() == ColumnClass.FACTOR) {
        metaInfo[i] = metaInfo[i].setFactor(columns.get(i).getOptionalFactor().getLevelMap());
      }
    }
    return metaInfo;
  }


  public static class MetaInfo implements Serializable {

    String header = null;
    String type;
    int columnNo = -1; // unset, base-1

    Map<String, Integer> factor;


    public MetaInfo(String header, String type) {
      this.header = header;
      this.type = type;
    }

    public MetaInfo(String header, String type, int colNo) {
      this(header, type);
      this.columnNo = colNo;
    }

    public String getHeader() {
      return header;
    }

    public MetaInfo setHeader(String header) {
      this.header = header;
      return this;
    }

    public String getType() {
      return type;
    }

    public MetaInfo setType(String type) {
      this.type = type;
      return this;
    }

    public int getColumnNo() {
      return this.columnNo;
    }

    public MetaInfo setColumnNo(int colNo) {
      this.columnNo = colNo;
      return this;
    }

    @Override
    public String toString() {
      return "MetaInfo [header=" + header + ", type=" + type + ", columnNo=" + columnNo + ", hasFactor=" + hasFactor()
          + "]";
    }

    public Map<String, Integer> getFactor() {
      return factor;
    }

    public MetaInfo setFactor(Map<String, Integer> factor) {
      this.factor = factor;
      return this;
    }

    public Boolean hasFactor() {
      return factor != null ? true : false;
    }

    public MetaInfo clone() {
      return new MetaInfo(header, type, columnNo).setFactor(factor);
    }
  }

}
