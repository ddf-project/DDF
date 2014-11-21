package io.spark.ddf.ml;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.spark.ddf.BaseTest;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.junit.Test;

public class ALSTest extends BaseTest {
  @Test
  public void TestALS() throws DDFException {
    createTableRatings();

    DDF ratings = manager.sql2ddf("select userid, movieid, score from ratings");
    int rank = 3;
    double lambda = 10;
    int iterNum = 15;

    MatrixFactorizationModel model = (MatrixFactorizationModel) ratings.ML.train("collaborativeFiltering", rank,
        iterNum, lambda).getRawModel();

    double r = model.predict(1, 4);
    System.out.println(">>>RATING: " + r);
    manager.shutdown();
  }
}
