package io.ddf.ml;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

public class AMLMetricsSupporter extends ADDFFunctionalGroupHandler implements ISupportMLMetrics {

  public AMLMetricsSupporter(DDF theDDF) {
    super(theDDF);
  }

  @Override
  public double r2score(double meanYTrue)
      throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DDF residuals() throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RocMetric roc(int alpha_length)
      throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double rmse(DDF testDDF, boolean implicitPref) throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

}
