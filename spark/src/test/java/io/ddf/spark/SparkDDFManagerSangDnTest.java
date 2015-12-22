package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sangdn on 12/18/15.
 */
public class SparkDDFManagerSangDnTest {
    static  DDFManager ddfManager;
    static String dsName;
    @BeforeClass
    public static void beforeClass(){
        try {
            ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
            dsName = "SparkSQL";
            createAndLoadNewAirlineTbl(dsName);
//            createNotLoadNewAirlineTbl(dsName);
//            createAndLoadNewAirlineFolderTbl(dsName);
        }catch(Exception ex){
            System.err.println("Couldn't init ddfManager");
            System.exit(-1);
        }
    }
    @AfterClass
    public static void afterClass(){
        try{
            ddfManager.shutdown();
        }catch (Exception ex){

        }
    }
    @Test
    public void testSql2DDF()throws DDFException {

        //createAndLoadNewAirlineTbl("MyDsName"); // will throw error :omg:

//        DDFManager ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
        SqlResult sql = ddfManager.sql("select * from airline", dsName);

        System.out.println("Schema: " +sql.getSchema());
        System.out.println(Arrays.toString(sql.getRows().toArray(new String[0])));

    }
    @Test
    public  void testDDF() throws DDFException {
        DDF ddf = ddfManager.sql2ddf("select * from airline", dsName);
        List<String> head = ddf.getViewHandler().head(10);
        System.out.println("Head 10: " + Arrays.toString(head.toArray()));
    }

    private static void createAndLoadNewAirlineTbl(String dataSourcename)throws DDFException{
//        DDFManager ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
        ddfManager.sql("drop table if exists airline", dataSourcename);

        ddfManager.sql("create external table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ", dataSourcename);

        ddfManager.sql("load data local inpath '../resources/test/airline.csv' into table airline", dataSourcename);

    }
    private static void createAndLoadNewAirlineFolderTbl(String dataSourcename)throws DDFException{
//        DDFManager ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
        ddfManager.sql("drop table if exists airline", dataSourcename);

        ddfManager.sql("create table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "LOCATION '../resources/test/double-airline/*'"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ", dataSourcename);

    }
    private static void createNotLoadNewAirlineTbl(String dataSourcename)throws DDFException{
//        DDFManager ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
        ddfManager.sql("drop table if exists airline", dataSourcename);

        ddfManager.sql("create external table airline (Year int,Month int,DayofMonth int,"
                + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
                + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
                + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
                + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
                + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
                + "CancellationCode string, Diverted string, CarrierDelay int, "
                + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int ) "
                + "LOCATION '../resources/test/airline.csv'"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ", dataSourcename);

    }
}
