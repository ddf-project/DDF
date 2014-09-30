package io.ddf.ml;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;
import io.ddf.misc.Config;
import io.ddf.ml.MLClassMethods.TrainMethod;
import io.ddf.util.Utils.MethodInfo;
import io.ddf.util.Utils.MethodInfo.ParamInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 */
public class MLSupporter extends ADDFFunctionalGroupHandler implements ISupportML {

  private Boolean sIsNonceInitialized = false;


  /**
   * For ML reflection test-case only
   */
  @Deprecated
  public MLSupporter() {
    super(null);
  }

  public MLSupporter(DDF theDDF) {
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



  // //// ISupportML //////

  /**
   * Runs a training algorithm on the entire DDF dataset.
   * 
   * @param trainMethodName
   * @param args
   * @return
   * @throws DDFException
   */
  @Override
  public IModel train(String trainMethodName, Object... paramArgs) throws DDFException {
    /**
     * Example signatures we must support:
     * <p/>
     * Unsupervised Training
     * <p/>
     * <code>
     * Kmeans.train(data: RDD[Array[Double]], k: Int, maxIterations: Int, runs: Int, initializationMode: String)
     * </code>
     * <p/>
     * Supervised Training
     * <p/>
     * <code>
     * LogisticRegressionWithSGD.train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, miniBatchFraction:
     * Double, initialWeights: Array[Double])
     * 
     * SVM.train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, regParam: Double, miniBatchFraction:
     * Double)
     * </code>
     */

    // Build the argument type array
    if (paramArgs == null) paramArgs = new Object[0];

    // Locate the training method
    String mappedName = Config.getValueWithGlobalDefault(this.getEngine(), trainMethodName);
    if (!Strings.isNullOrEmpty(mappedName)) trainMethodName = mappedName;

    TrainMethod trainMethod = new TrainMethod(trainMethodName, MLClassMethods.DEFAULT_TRAIN_METHOD_NAME, paramArgs);
    if (trainMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s", trainMethodName));
    }

    // Now we need to map the DDF and its column specs to the input format expected by the method we're invoking
    Object[] allArgs = this.buildArgsForMethod(trainMethod.getMethod(), paramArgs);

    // Invoke the training method
    Object rawModel = trainMethod.classInvoke(allArgs);


    List<Schema.Column> columns = this.getDDF().getSchemaHandler().getColumns();
    String[] trainedColumns = new String[columns.size()];

    for (int i = 0; i < columns.size(); i++) {
      trainedColumns[i] = columns.get(i).getName();
    }

    for (String col : trainedColumns) {
      mLog.info(">>>>>> trainedCol = " + col);
    }

    IModel model = new Model(rawModel);
    model.setTrainedColumns(trainedColumns);
    mLog.info(">>>> modelID = " + model.getName());
    this.getManager().addModel(model);
    return model;
  }


  @SuppressWarnings("unchecked")
  private Object[] buildArgsForMethod(Method method, Object[] paramArgs) throws DDFException {
    MethodInfo methodInfo = new MethodInfo(method);
    List<ParamInfo> paramInfos = methodInfo.getParamInfos();
    if (paramInfos == null || paramInfos.isEmpty()) return new Object[0];

    Object firstParam = this.convertDDF(paramInfos.get(0));

    if (paramArgs == null || paramArgs.length == 0) {
      return new Object[] { firstParam };

    } else {
      List<Object> result = Lists.newArrayList();
      result.add(firstParam);
      result.addAll(Arrays.asList(paramArgs));
      return result.toArray(new Object[0]);
    }
  }

  /**
   * Override this to return the appropriate DDF representation matching that specified in {@link ParamInfo}. The base
   * implementation simply returns the DDF.
   * 
   * @param paramInfo
   * @return
   */
  protected Object convertDDF(ParamInfo paramInfo) throws DDFException {
    return this.getDDF();
  }



  /**
   * Base implementation does nothing
   * 
   * @return the original, unmodified DDF
   */
  @Override
  public DDF applyModel(IModel model) throws DDFException {
    return this.getDDF();
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels) throws DDFException {
    return this.getDDF();
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    return this.getDDF();
  }

  @Override
  public long[][] getConfusionMatrix(IModel model, double threshold) throws DDFException {
    return null;
  }

  public List<List<DDF>> CVKFold(int k, Long seed) throws DDFException {
    return new ArrayList<List<DDF>>();
  }

  @Override
  public List<List<DDF>> CVRandom(int k, double trainingSize, Long seed) throws DDFException {
    return new ArrayList<List<DDF>>();
  }
}
