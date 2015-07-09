package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.TableNameReplacer;
import io.ddf.content.Schema;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;


/**
 * Created by jing on 6/30/15.
 */
public class TableNameReplacerTests {




    public static DDFManager manager;
    public static CCJSqlParserManager parser;

    @Test
    public void testRun() throws DDFException {
        /**
        createTableAirline();
        DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
                + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");

        manager.setDDFName(ddf, "myddf");
        Assert.assertEquals("ddf://adatao/" + ddf.getName(), ddf.getUri());

        manager.addDDF(ddf);
        Assert.assertEquals(ddf, manager.getDDF(ddf.getUUID()));

        // ddf.getSummary() check what's this
         */
    }


    @Test
    public void testCondition0() throws  DDFException {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        // String sqlcmd = "describe table ddf://adatao/a";
        String sqlcmd = "select sum(depdelay) from ddf://adatao/airline group by year order by year";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }
        tableNameReplacer.run(statement);
        System.out.println(statement.toString());
    }

    @Test
    public  void testCondition1() throws  DDFException {
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager);
        String sqlcmd = "select SUM(ddf://adatao/a.b) from ddf://adatao/a group by ddf://adatao/a.a";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }

        tableNameReplacer.run(statement);
        System.out.println(statement.toString());
    }

    @Test
    public void testCondition2() throws  DDFException {
        TableNameReplacer tableNameReplacer  = new TableNameReplacer(manager, "adatao");
        String sqlcmd = "select a.b from a";
        Statement statement = null;
        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }
        tableNameReplacer.run(statement);
        System.out.println(statement.toString());

    }

    @Test
    public void testCondition3() throws  DDFException {
        String[] uris={"ddf://adatao/a", "ddf://adatao/b"};
        TableNameReplacer tableNameReplacer = new TableNameReplacer(manager, Arrays.asList(uris));
        String sqlcmd = "select {1}.a,{2}.b from {1}";
        Statement statement = null;

        try {
            statement = parser.parse(new StringReader(sqlcmd));
        } catch (JSQLParserException e) {
            e.printStackTrace();
            assert(false);
        }

        tableNameReplacer.run(statement);
        System.out.println(statement.toString());
    }

    @Test
    public  void testLoading() throws  DDFException {
        manager.sql("drop table if exists airline");

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
               + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '/Users/Jing/Github/DDF/resources/test/airline.csv' into table airline");

        DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
                + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
        this.manager.setDDFName(ddf, "airlineDDF");
        //String[] testStr={"ddf://adatao/airlineDDF"};
        //SqlResult ret = manager.sql("select SUM({1}.year) from {1}", null, null, Arrays.asList(testStr));
        //System.out.println(ddf.getUri());
        //System.out.println(ddf.getTableName());
        //SqlResult ret = manager.sql("show tables");

        //SqlResult describeRet = manager.sql("describe table ddf://adatao/airlineDDF");

        DDF sql2ddfRet = manager.sql2ddf("select * from ddf://adatao/airlineDDF");
        // DDF sql2ddfRet = manager.sql2ddf("select * from ddf://adatao/airlineDDF");
        // DDF sql2ddfRet = manager.sql2ddf("select * from ddf://adatao/airlineDDF");
        int a = 1;
    }

    // static Logger LOG;


    @BeforeClass
    public static void startServer() throws Exception {
        Thread.sleep(1000);
        // LOG = LoggerFactory.getLogger(BaseTest.class);
        manager = DDFManager.get("spark");
        Schema schema = new Schema("tablename1", "d  d,d  d");
        DDF ddf = manager.newDDF(manager, new Class<?>[] { DDFManager.class }, "adatao", "a",
                schema);
        Schema schema2 = new Schema("tablename2", "d  d,d  d");
        DDF ddf2 = manager.newDDF(manager, new Class<?>[] { DDFManager.class }, "adatao", "b",
                schema2);

       // manager.addDDF(null);
        manager.sql("show tables");
        parser = new CCJSqlParserManager();
    }

    @AfterClass
    public static void stopServer() throws Exception {

        manager.shutdown();
    }

    public void createTableAirline() throws DDFException {
        manager.sql("drop table if exists airline");

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '../resources/test/airline.csv' into table airline");
        DDF ddf = manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
                + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");


    }

    public void createTableSmiths2() throws DDFException {
        manager.sql("drop table if exists smiths2");

        manager.sql("create table smiths2 (subject string, variable string, value double) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '../resources/test/smiths2' into table smiths2");

    }

    public void createTableAirlineWithNA() throws DDFException {
        manager.sql("drop table if exists airline");

        manager.sql("create table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '../resources/test/airlineWithNA.csv' into table airline");
    }

    public void createTableMovie() throws DDFException {
        manager.sql("drop table if exists movie");

        manager.sql("create table movie (" +
                "idx string,title string,year int,length int,budget double,rating double,votes int," +
                "r1 double,r2 double,r3 double,r4 double,r5 double,r6 double,r7 double,r8 double,r9 double,r10 double," +
                "mpaa string,Action int,Animation int,Comedy int,Drama int,Documentary int,Romance int,Short int" +
                ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '../resources/test/movies.csv' into table movie");
    }

    public void createTableRatings() throws DDFException {
        manager.sql("drop table if exists ratings");

        manager.sql("create table ratings (userid int,movieid int,score double ) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

        manager.sql("load data local inpath '../resources/test/ratings.data' into table ratings");
    }

    public void createTableMtcars() throws DDFException {
        manager.sql("drop table if exists mtcars");

        manager.sql("CREATE TABLE mtcars ("
                + "mpg double, cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
                + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '");

        manager.sql("load data local inpath '../resources/test/mtcars' into table mtcars");
    }

}