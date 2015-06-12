package io.ddf.analytics;


/**
 * Created by huandao on 6/11/15.
 */
public class NumericSimpleSummary extends SimpleSummary {
  private double mMin;
  private double mMax;

  public void setMin(double min) {
    this.mMin = min;
  }

  public void setMax(double max) {
    this.mMax = max;
  }

  public double getMin() {
    return this.mMin;
  }

  public double getMax() {
    return this.mMax;
  }
}
