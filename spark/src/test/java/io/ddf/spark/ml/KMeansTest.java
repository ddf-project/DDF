package io.ddf.spark.ml;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.spark.BaseTest;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.junit.Assert;
import org.junit.Test;

public class KMeansTest extends BaseTest {
  @Test
  public void TestKMeans() throws DDFException {
    createTableAirline();

    DDF ddf = manager.sql2ddf("select deptime, arrtime, distance, depdelay, arrdelay from airline");

    KMeansModel kmeansModel = (KMeansModel) ddf.ML.KMeans(5, 5, 2, "random").getRawModel();
    Assert.assertEquals(5, kmeansModel.clusterCenters().length);
    // Assert.assertTrue(kmeansModel.computeCost((RDD<double[]>)ddf.getRepresentationHandler().get(
    // RDD_ARR_DOUBLE().getTypeSpecsString())) > 0);
    // Assert.assertTrue(kmeansModel.predict(new double[] { 1232, 1341, 389, 7, 1 }) > -1);
    // Assert.assertTrue(kmeansModel.predict(new double[] { 1232, 1341, 389, 7, 1 }) < 5);

  }


}
