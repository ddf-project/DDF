package io.spark.ddf.ml;


import static io.spark.ddf.content.RepresentationHandler.RDD_ARR_DOUBLE;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import org.junit.Test;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

public class KMeansTest {
  @Test
  public void TestKMeans() throws DDFException {
    DDFManager manager = DDFManager.get("spark");

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

    DDF ddf = manager.sql2ddf("select deptime, arrtime, distance, depdelay, arrdelay from airline");
    int k = 5;
    int numIterations = 5;
    KMeansModel kmeansModel = (KMeansModel) ddf.ML.train("kmeans", k, numIterations).getRawModel();
    Assert.assertEquals(5, kmeansModel.clusterCenters().length);
    Assert.assertTrue(kmeansModel.computeCost((RDD<double[]>) ddf.getRepresentationHandler().get(
        RDD_ARR_DOUBLE().getTypeSpecsString())) > 0);
    Assert.assertTrue(kmeansModel.predict(new double[] { 1232, 1341, 389, 7, 1 }) > -1);
    Assert.assertTrue(kmeansModel.predict(new double[] { 1232, 1341, 389, 7, 1 }) < 5);

    manager.shutdown();
  }


}
