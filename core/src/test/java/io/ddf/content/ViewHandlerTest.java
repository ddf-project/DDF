package io.ddf.content;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import java.util.List;

import junit.framework.Assert;
import org.junit.Test;

public class ViewHandlerTest {

  @Test
  public void testSubset() {
    //TODO: get expression string
  }

  @Test
  public void testRemoveColumn() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    try {
      manager.sql2txt("drop table if exists airline");
    } catch (Exception e) {
      System.out.println(e);
    }

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/airline.csv' into table airline");

    DDF ddf = manager.sql2ddf("select * from airline");

    List<String> columns = Lists.newArrayList();
    columns.add("year");
    columns.add("month");
    columns.add("deptime");

    DDF newddf1 = ddf.VIEWS.removeColumn("year");
    DDF newddf2 = ddf.VIEWS.removeColumns("year", "deptime");
    DDF newddf3 = ddf.VIEWS.removeColumns(columns);

    Assert.assertEquals(28, newddf1.getNumColumns());
    Assert.assertEquals(27, newddf2.getNumColumns());
    Assert.assertEquals(26, newddf3.getNumColumns());
  }
}
