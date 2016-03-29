package io.ddf.spark;

import io.ddf.exception.DDFException;
import io.ddf.spark.util.LongAccumulableParam;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.codehaus.janino.Java;
import org.junit.Test;


import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Boolean;
import scala.Function1;
import scala.Tuple2;

/**
 * Created by sangdn on 25/03/2016.
 */
public class LongAccumulableParamTest extends BaseTest implements Serializable{

  public void createTableAirlineAllString() throws DDFException {
    manager.sql("drop table if exists airline", "SparkSQL");

      manager.sql("create table airline (Year string,Month DECIMAL,DayofMonth string,"
        + "DayOfWeek string,DepTime string,CRSDepTime string,ArrTime string,"
        + "CRSArrTime string,UniqueCarrier string, FlightNum string, "
        + "TailNum string, ActualElapsedTime string, CRSElapsedTime string, "
        + "AirTime string, ArrDelay string, DepDelay string, Origin string, "
        + "Dest string, Distance string, TaxiIn string, TaxiOut string, Cancelled string, "
        + "CancellationCode string, Diverted string, CarrierDelay string, "
        + "WeatherDelay string, NASDelay string, SecurityDelay string, LateAircraftDelay string ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", "SparkSQL");

    manager.sql("load data local inpath '../resources/test/airline.csv' into table airline", "SparkSQL");

  }

  @Test
  public void testSimpleSparkDDFManager() throws DDFException {

    createTableAirlineAllString();


    SparkDDF ddf = (SparkDDF) manager.sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, "
        + "depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline", "SparkSQL");

    JavaRDD<Row> rdd = ddf.getRDD(Row.class).toJavaRDD();

    SparkDDFManager manager = (SparkDDFManager) ddf.getManager();
    SparkContext sparkContext = manager.getSparkContext();
    Accumulator<Long> totalLineFailed = sparkContext.accumulator(0L, LongAccumulableParam.getInstance());
    Accumulator<Long> totalLineSuccess = sparkContext.accumulator(0L, LongAccumulableParam.getInstance());

    Tuple2<Accumulator<Long>, Accumulator<Long>> errorInfo = new Tuple2<>(totalLineFailed, totalLineSuccess);

//    RDD<Row> filter = rdd.filter((r) -> isAppliable(errorInfo,r));
    JavaRDD<Row> filter = rdd.filter( (row) -> filter(row,errorInfo));
    long totalLine=filter.count();
    System.out.println("Origin RDD");
    List<Row> take = rdd.take(10);
    for (Row row : take) {
      System.out.println(row.toString());
    }
    System.out.println("After Apply Schema RDD NumRow/NumError:" + errorInfo._1().value() + "/" + errorInfo._2().value());

    List<Row>rows = filter.take(10);
    for (Row row : rows) {
      System.out.println(row.toString());
    }
    System.out.println("After Apply Schema RDD NumRow/NumError:" + errorInfo._1().value() + "/" + errorInfo._2().value());


  }

  public boolean filter(Row row,  Tuple2<Accumulator<Long>, Accumulator<Long>> errorInfo){
    errorInfo._2().add(1L);
    return true;
  }




}
