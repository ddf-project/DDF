package io.spark.ddf;


import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {
  public static DDFManager manager;

  static Logger LOG;


  @BeforeClass
  public static void startServer() throws Exception {
    LOG = LoggerFactory.getLogger(BaseTest.class);
    manager = DDFManager.get("spark");

  }

  @AfterClass
  public static void stopServer() throws Exception {

    manager.shutdown();
  }

  public void createTableAirline() throws DDFException {
    manager.sql2txt("drop table if exists airline");

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

  }

  public void createTableAirlineWithNA() throws DDFException {
    manager.sql2txt("drop table if exists airline");

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/airlineWithNA.csv' into table airline");
  }
  
  public void createTableRatings() throws DDFException {
    manager.sql2txt("drop table if exists ratings");

    manager.sql2txt("create table ratings (userid int,movieid int,score double ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/ratings.data' into table ratings");
  }
}
