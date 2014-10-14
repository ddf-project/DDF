package io.spark.ddf.ml;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.junit.Test;

public class ALSTest {
  @Test
  public void TestALS() throws DDFException {
    DDFManager manager = DDFManager.get("spark");

    manager.sql2txt("drop table if exists ratings");

    manager.sql2txt("create table ratings (userid int,movieid int,score double ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");

    manager.sql2txt("load data local inpath '../resources/test/ratings.data' into table ratings");

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
