package io.ddf.spark;


import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {
    public static DDFManager manager;

    public static Logger LOG;


    @BeforeClass
    public static void startServer() throws Exception {
        Thread.sleep(1000);
        LOG = LoggerFactory.getLogger(BaseTest.class);
        manager = DDFManager.get(DDFManager.EngineType.SPARK);
    }

    @AfterClass
    public static void stopServer() throws Exception {

        manager.shutdown();
    }

    public void createTableAirline() throws DDFException {
        manager.sql("drop table if exists airline", "SparkSQL");

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
            + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
            + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
            + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
            + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
            + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
            + "CancellationCode string, Diverted string, CarrierDelay int, "
            + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/airline.csv' into table airline", "SparkSQL");

    }

    public void createTableAirlineBigInt() throws DDFException {
        manager.sql("drop table if exists airline_bigint", "SparkSQL");

        manager.sql("create table airline_bigint (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay bigint, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/airline.csv' into table airline_bigint", "SparkSQL");

    }

    public void createTableSmiths2() throws DDFException {
        manager.sql("drop table if exists smiths2", "SparkSQL");

        manager.sql("create table smiths2 (subject string, variable string, value double) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/smiths2' into table smiths2", "SparkSQL");

    }

    public void createTableAirlineWithNA() throws DDFException {
        manager.sql("drop table if exists airline", "SparkSQL");

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
            + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
            + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
            + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
            + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
            + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
            + "CancellationCode string, Diverted string, CarrierDelay int, "
            + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/airlineWithNA.csv' into table airline", "SparkSQL");
    }

    public void createTableMovie() throws DDFException {
        manager.sql("drop table if exists movie", "SparkSQL");

        manager.sql("create table movie (" +
            "idx string,title string,year int,length int,budget double,rating double,votes int," +
            "r1 double,r2 double,r3 double,r4 double,r5 double,r6 double,r7 double,r8 double,r9 double,r10 double," +
            "mpaa string,Action int,Animation int,Comedy int,Drama int,Documentary int,Romance int,Short int" +
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/movies.csv' into table movie", "SparkSQL");
    }

    public void createTableRatings() throws DDFException {
        manager.sql("drop table if exists ratings", "SparkSQL");

        manager.sql("create table ratings (userid int,movieid int,score double ) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/ratings.data' into table ratings", "SparkSQL");
    }

    public void createTableMtcars() throws DDFException {
        manager.sql("drop table if exists mtcars", "SparkSQL");

        manager.sql("CREATE TABLE mtcars ("
            + "mpg double, cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
            + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/mtcars' into table mtcars", "SparkSQL");
    }
    
    public void createTableCarOwner() throws DDFException {
      manager.sql("drop table if exists carowner", "SparkSQL");

      manager.sql("CREATE TABLE carowner ("
          + "name string, cyl int, disp double"
          + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '", "SparkSQL");

      manager.sql("load data local inpath '../resources/test/carowner' into table carowner", "SparkSQL");
    }
    
    public static void createTableStocks() throws DDFException {
      
      manager.sql("drop table if exists stocks", "SparkSQL");
      manager.sql("create table stocks (Symbol string, Date string, Open double, High double, Low double, Close double, Volume double, AdjustedClose double)"
          + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");
      
      manager.sql("load data local inpath '../resources/test/quandl_stocks.csv' into table stocks", "SparkSQL");
    }

    public void createTableStocksSmall() throws DDFException {
        manager.sql("drop table if exists stocks_small", "SparkSQL");
        manager.sql("create table stocks_small (Symbol string, Date string, Open double, High double, Low double, Close double, Volume double, AdjustedClose double)"
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

        manager.sql("load data local inpath '../resources/test/quandl_stocks_small.csv' into table stocks_small", "SparkSQL");
    }
}
