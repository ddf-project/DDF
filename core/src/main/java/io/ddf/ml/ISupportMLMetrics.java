package io.ddf.ml;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

public interface ISupportMLMetrics extends IHandleDDFFunctionalGroup {

  public double r2score(double meanYTrue) throws DDFException;

  public DDF residuals() throws DDFException;

  public RocMetric roc(int alpha_length) throws DDFException;

  public double rmse(DDF predictionDDF, boolean implicitPref) throws DDFException;
}
