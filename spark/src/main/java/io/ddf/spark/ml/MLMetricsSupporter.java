package io.ddf.spark.ml;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.ml.AMLMetricsSupporter;
import io.ddf.ml.RocMetric;
import io.ddf.spark.SparkDDF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.List;

public class MLMetricsSupporter extends AMLMetricsSupporter {

  private Boolean sIsNonceInitialized = false;

  @Override
  /*
   * input: prediction DDF input: meanYtrue
   * 
   * output: r2score in double
   * 
   * (non-Javadoc)
   * 
   * @see io.ddf.ml.AMLMetricsSupporter#r2score(io.ddf.DDF, double)
   */
  public double r2score(double meanYTrue) throws DDFException {
    SparkDDF predictionDDF = (SparkDDF) this.handleCategoricalColumns();
    JavaRDD<double[]> resultRDD = predictionDDF.getJavaRDD(double[].class);

    double[] result = (double[]) resultRDD.map(new MetricsMapperR2(meanYTrue)).reduce(new MetricsReducerR2());
    if (result == null || result.length == 0) {
      throw new DDFException("R2score result returns null");
    }
    double sstot = result[0];
    double ssres = result[1];

    if (sstot == 0) {
      return 1;
    } else {
      return 1 - (ssres / sstot);
    }
  }


  public static class MetricsMapperR2 implements Function<double[], double[]> {
    private static final long serialVersionUID = 1L;
    public double meanYTrue = -1.0;


    public MetricsMapperR2(double _meanYTrue) throws DDFException {
      this.meanYTrue = _meanYTrue;
    }

    public double[] call(double[] input) throws Exception {
      double[] outputRow = new double[2];

      if (input instanceof double[] && input.length > 1) {
        double yTrue = input[0];
        double yPredict = input[1];
        outputRow[0] = (yTrue - meanYTrue) * (yTrue - meanYTrue);
        outputRow[1] = (yTrue - yPredict) * (yTrue - yPredict);


      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }


  public static class MetricsReducerR2 implements Function2<double[], double[], double[]> {
    private static final long serialVersionUID = 1L;


    public MetricsReducerR2() throws DDFException {
    }

    @Override
    public double[] call(double[] arg0, double[] arg1) throws Exception {
      double[] outputRow = new double[2];
      if (arg0 instanceof double[] && arg0.length > 1) {
        outputRow[0] = arg0[0] + arg1[0];
        outputRow[1] = arg0[1] + arg1[1];
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }


  @Override
  public DDF residuals() throws DDFException {
    SparkDDF predictionDDF = (SparkDDF) this.handleCategoricalColumns();
    JavaRDD<double[]> predictionRDD = predictionDDF.getJavaRDD(double[].class);

    JavaRDD<double[]> result = predictionRDD.map(new MetricsMapperResiduals());

    if (result == null) mLog.error(">> javaRDD result of MetricMapper residuals is null");
    if (predictionDDF.getManager() == null) mLog.error(">> predictionDDF.getManager() is null");
    if (result.rdd() == null) mLog.error(">> result.rdd() is null");
    if (predictionDDF.getNamespace() == null) mLog.error(">> predictionDDF.getNamespace() is null");
    if (predictionDDF.getSchema() == null) mLog.error(">> predictionDDF.getSchema() is null");
    if (predictionDDF.getName() == null) mLog.error(">> predictionDDF.getName() is null");

    Schema schema = new Schema("residuals double");
    DDFManager manager = this.getDDF().getManager();
    DDF residualDDF = manager
        .newDDF(manager, result.rdd(), new Class<?>[] { RDD.class, double[].class }, predictionDDF.getNamespace(), null,
            schema);

    if (residualDDF == null) mLog.error(">>>>>>>>>>>.residualDDF is null");

    return residualDDF;
  }

  public static class MetricsMapperResiduals implements Function<double[], double[]> {
    private static final long serialVersionUID = 1L;

    public MetricsMapperResiduals() throws DDFException {
    }

    public double[] call(double[] input) throws Exception {
      double[] outputRow = new double[1];

      if (input instanceof double[] && input.length > 1) {
        double yTrue = input[0];
        double yPredict = input[1];
        outputRow[0] = outputRow[0] = (yTrue - yPredict);
      } else {
        throw new DDFException(String.format("Unsupported input type "));
      }
      return outputRow;
    }
  }

  @Override
  /*
   * input expected RDD[double[][]]
   * (non-Javadoc)
   * @see io.ddf.ml.AMLMetricsSupporter#roc(io.ddf.DDF, int)
   */
  public RocMetric roc(int alpha_length) throws DDFException {
    DDF predictionDDF = this.handleCategoricalColumns();
    RDD<LabeledPoint> rddLabeledPoint = (RDD<LabeledPoint>) predictionDDF.getRepresentationHandler()
        .get(RDD.class, LabeledPoint.class);
    ROCComputer rc = new ROCComputer();

    return (rc.ROC(rddLabeledPoint, alpha_length));
  }

  public double rmse(DDF predictedDDF, boolean implicitPrefs) throws DDFException {
    RDD<Rating> predictions = (RDD<Rating>) predictedDDF.getRepresentationHandler().get(RDD.class, Rating.class);
    RDD<Rating> ratings = (RDD<Rating>) this.getDDF().getRepresentationHandler().get(RDD.class, Rating.class);
    return new ROCComputer().computeRmse(ratings, predictions, false);
  }

  public MLMetricsSupporter(DDF theDDF) {
    super(theDDF);
    this.initialize();
  }

  private void initialize() {
    if (sIsNonceInitialized) return;

    synchronized (sIsNonceInitialized) {
      if (sIsNonceInitialized) return;
      sIsNonceInitialized = true;

      this.initializeConfiguration();
    }
  }

  /**
   * Optional: put in any hard-coded mapping configuration here
   */
  private void initializeConfiguration() {
    // if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans"))) {
    // Config.set(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans",
    // String.format("%s#%s", MLSupporter.class.getName(), "dummyKMeans"));
    // }
  }

  protected DDF handleCategoricalColumns() throws DDFException {
    List<String> factorColumns = new ArrayList<String>();
    for(Schema.Column column: this.getDDF().getSchema().getColumns()) {
      if(column.getColumnClass() == Schema.ColumnClass.FACTOR) {
        factorColumns.add(column.getName());
      }
    }
    if(factorColumns.size() > 0) {
      return this.getDDF().getTransformationHandler().factorIndexer(factorColumns);
    } else {
      return this.getDDF();
    }
  }
}
