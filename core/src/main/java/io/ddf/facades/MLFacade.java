/**
 *
 */
package io.ddf.facades;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.ml.IModel;
import io.ddf.ml.ISupportML;

import java.util.List;

/**
 * A helper class to group together the various ML functions that would otherwise crowd up DDF.java
 */
public class MLFacade implements ISupportML {

  private DDF mDDF;
  private ISupportML mMLSupporter;


  public MLFacade(DDF ddf, ISupportML mlSupporter) {
    mDDF = ddf;
    mMLSupporter = mlSupporter;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public ISupportML getMLSupporter() {
    return mMLSupporter;
  }

  public void setMLSupporter(ISupportML mlSupporter) {
    mMLSupporter = mlSupporter;
  }


  @Override
  public IModel train(String trainMethodName, Object... params) throws DDFException {
    return this.getMLSupporter().train(trainMethodName, params);
  }

  @Override
  public DDF applyModel(IModel model) throws DDFException {
    return this.getMLSupporter().applyModel(model);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels) throws DDFException {
    return this.getMLSupporter().applyModel(model, hasLabels);
  }

  @Override
  public DDF applyModel(IModel model, boolean hasLabels, boolean includeFeatures) throws DDFException {
    return this.getMLSupporter().applyModel(model, hasLabels, includeFeatures);
  }

  // //// Convenient facade ML algorithm names //////

  public IModel KMeans(int numCentroids, int maxIters, int runs, String initMode)
      throws DDFException {
    return this.train("kmeans", numCentroids, maxIters, runs, initMode);
  }

  public IModel KMeans(int numCentroids, int maxIters, int runs) throws DDFException {
    return this.train("kmeans", numCentroids, maxIters, runs);
  }

  public long[][] getConfusionMatrix(IModel model, double threshold) throws DDFException {
    return this.getMLSupporter().getConfusionMatrix(model, threshold);
  }

  public List<List<DDF>> CVKFold(int k, Long seed) throws DDFException {
    return this.getMLSupporter().CVKFold(k, seed);
  }

  public List<List<DDF>> CVRandom(int k, double trainingSize, Long seed) throws DDFException {
    return this.getMLSupporter().CVRandom(k, trainingSize, seed);
  }

  public IModel als(int rank, int iteration, double lamda) throws DDFException {
    return this.train("collaborativeFiltering", rank, iteration, lamda);
  }

}
