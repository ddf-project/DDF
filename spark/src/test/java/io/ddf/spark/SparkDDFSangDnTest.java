package io.ddf.spark;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.IHandleAggregation;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sangdn on 12/23/15.
 */
public class SparkDDFSangDnTest {
    static DDFManager ddfManager;
    static String dsName;
    

    @BeforeClass
    public static void beforeClass() {
        try {
            ddfManager = DDFManager.get(DDFManager.EngineType.SPARK);
            dsName = "SparkSQL";
            createAndLoadNewAirlineTbl(dsName);
//            createNotLoadNewAirlineTbl(dsName);
//            createAndLoadNewAirlineFolderTbl(dsName);
//            ddf = ddfManager.sql2ddf("select * from airline");
        } catch (Exception ex) {
            System.err.println("Couldn't init ddfManager");
            System.exit(-1);
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            ddfManager.shutdown();
        } catch (Exception ex) {

        }
    }

    @Test
    public void showDDFBasicInfo() throws DDFException {
        DDF ddf = ddfManager.sql2ddf("select * from airline", dsName);
        System.out.println("Column Name: " + Arrays.toString(ddf.getColumnNames().toArray()));
        System.out.println("Created Time : " + ddf.getCreatedTime());
        System.out.println("Engine : " + ddf.getEngine());
        System.out.println("4 Num Summary: " + Arrays.toString(ddf.getFiveNumSummary()));
        System.out.println("ddf Name: " +ddf.getName());
        System.out.println("ddf NameSpace: " +ddf.getNamespace());
        System.out.println("ddf NumColumn: " +ddf.getNumColumns());
        System.out.println("ddf Num Rows: " +ddf.getNumRows());
        System.out.println("ddf Table Name: " +ddf.getTableName());
        System.out.println("ddf URI: " +ddf.getUri());
        System.out.println("ddf Schema: " +ddf.getSchema().toString());
        System.out.println("ddf Summary: " +Arrays.toString(ddf.getSummary()));
    }
    @Test
    public void testAggregationHandler() throws DDFException {
        DDF ddf = ddfManager.sql2ddf("select * from airline", dsName);
        System.out.println(" Top 10: " + Arrays.toString(ddf.VIEWS.head(10).toArray()));
    }

    private static void createAndLoadNewAirlineTbl(String dataSourcename) throws DDFException {
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
}
