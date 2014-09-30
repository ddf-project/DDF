package io.ddf.analytics;


import java.io.Serializable;
import io.ddf.util.Utils;
import com.google.common.base.Joiner;

/**
 * Basic statistics for a set of double numbers including min, max, count, NAcount, mean, variance and stdev
 * 
 * @author bhan
 * 
 */
@SuppressWarnings("serial")
public class Summary implements Serializable {

  private long mCount = 0; // tracking number of non-NA values
  private double mMean = 0; // tracking mean
  private double mSS = 0; // sum of squared deviations
  private long mNACount = 0; // tracking number of NA values
  private double mMin = Double.MAX_VALUE;
  private double mMax = Double.MIN_VALUE;


  public Summary() {}

  public Summary(double[] numbers) {
    this.merge(numbers);
  }

  public Summary(long mCount, double mMean, double mSS, long mNACount, double mMin, double mMax) {
    super();
    this.mCount = mCount;
    this.mMean = mMean;
    this.mSS = mSS;
    this.mNACount = mNACount;
    this.mMin = mMin;
    this.mMax = mMax;
  }

  public Summary newSummary(Summary a) {
    return new Summary(a.mCount, a.mMean, a.mSS, a.mNACount, a.mMin, a.mMax);
  }

  public long count() {
    return this.mCount;
  }

  public long NACount() {
    return this.mNACount;
  }

  public double mean() {
    if (mCount == 0) return Double.NaN;
    return this.mMean;
  }

  public double mSS() {
    return this.mSS;
  }

  public boolean isNA() {
    return (this.mCount == 0 && this.mNACount > 0);
  }

  public void setNACount(long n) {
    this.mNACount = n;
  }

  public double min() {
    if (this.mCount == 0) return Double.NaN;
    return this.mMin;
  }

  public double max() {
    if (this.mCount == 0) return Double.NaN;
    return this.mMax;
  }

  public void addToNACount(long number) {
    this.mNACount += number;
  }

  public Summary merge(double number) {
    if (Double.isNaN(number)) {
      this.mNACount++;
    } else {
      this.mCount++;
      double delta = number - mMean;
      mMean += delta / mCount;
      mSS += delta * (number - mMean);
      mMin = Math.min(mMin, number);
      mMax = Math.max(mMax, number);
    }
    return this;
  }

  public Summary merge(double[] numbers) {
    for (double number : numbers) {
      this.merge(number);
    }
    return this;
  }

  public Summary merge(Summary other) {
    if (this.equals(other)) {
      return merge(newSummary(other));// for self merge
    } else {
      if (mCount == 0) {
        mMean = other.mean();
        mSS = other.mSS();
        mCount = other.count();
      } else if (other.mCount != 0) {
        double delta = other.mean() - mMean;
        long n = (mCount + other.mCount);
        if (other.mCount * 10 < mCount) {
          mMean = mMean + (delta * other.mCount) / n;
        } else if (mMean * 10 < other.mean()) {
          mMean = other.mean() - (delta * mCount) / n;
        } else {
          mMean = (mMean * mCount + other.mean() * other.mCount) / n;
        }
        mSS += other.mSS() + (delta * delta * mCount * other.mCount) / n;
        mCount += other.mCount;
      }
      this.mNACount += other.NACount();
      this.mMin = Math.min(this.mMin, other.mMin);
      this.mMax = Math.max(this.mMax, other.mMax);
      return this;
    }

  }

  public double sum() {
    return mMean * mCount;
  }

  public double variance() {
    if (mCount <= 1) {
      return Double.NaN;
    } else {
      return mSS / (mCount - 1);
    }
  }

  public double stdev() {
    return Math.sqrt(this.variance());
  }

  @Override
  public String toString() {
    Joiner joiner = Joiner.on("");
    return joiner.join("mean:", Utils.roundUp(mean()), " stdev:", Utils.roundUp(stdev()), " var:",
        Utils.roundUp(variance()), " cNA:", mNACount, " count:", mCount, " min:", Utils.roundUp(min()), " max:",
        Utils.roundUp(max()));
  }
}
