package io.ddf.ml;


import io.ddf.DDF;

/**
 */
public class CrossValidationSet {

  private DDF mTrainSet;

  private DDF mTestSet;

  public CrossValidationSet(DDF train, DDF test) {
    this.mTrainSet = train;
    this.mTestSet = test;
  }

  public DDF getTrainSet() {
    return mTrainSet;
  }

  public DDF getTestSet() {
    return mTestSet;
  }

  public void setTrainSet(DDF ddf) {
    mTrainSet = ddf;
  }

  public void setTestSet(DDF ddf) {
    mTestSet = ddf;
  }
}
