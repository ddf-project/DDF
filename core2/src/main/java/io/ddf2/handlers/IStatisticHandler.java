package io.ddf2.handlers;

import com.google.common.base.Joiner;
import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.Utils;


import java.io.Serializable;
import java.util.List;

public interface IStatisticHandler<T extends DDF<T>> extends IDDFHandler<T> {
    /**
     * Get summary for the ddf.
     *
     * @return
     * @throws DDFException
     */
    Summary[] getSummary() throws DDFException;

    /**
     * Get min/max for numeric columns, list of distinct values for categorical columns
     *
     * @return
     * @throws DDFException
     */
    SimpleSummary[] getSimpleSummary() throws DDFException;

    /**
     * Get five num (0, 0.25, 0.5, 0.75 and 1 percentile) summary
     *
     * @return
     * @throws DDFException
     */
    FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

    /**
     * Get values at the given percentiles.
     *
     * @param columnName  The name of the column.
     * @param percentiles The percentiles.
     * @return
     * @throws DDFException
     */
    Double[] getQuantiles(String columnName, Double[] percentiles) throws DDFException;

    /**
     * Get variance for the column data.
     *
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    Double[] getVariance(String columnName) throws DDFException;

    /**
     * Get mean value for the column name.
     *
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    Double getMean(String columnName) throws DDFException;

    /**
     * Get correlation between the two columns.
     *
     * @param xColumnName The first column name.
     * @param yColumnName The second column name.
     * @return
     * @throws DDFException
     */
    Double getCor(String xColumnName, String yColumnName) throws DDFException;

    /**
     * Get covariance between the two columns.
     *
     * @param xColumnName The first column name.
     * @param yColumnName The second column name.
     * @return
     * @throws DDFException
     */
    Double getCovariance(String xColumnName, String yColumnName) throws DDFException;

    /**
     * Get min value for the column.
     *
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    Double getMin(String columnName) throws DDFException;

    /**
     * Get max value for the column.
     *
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    Double getMax(String columnName) throws DDFException;


    /**
     * Basic statistics for a set of double numbers including min, max, count,
     * NAcount, mean, variance and stdev
     */
    @SuppressWarnings("serial")
    public class Summary implements Serializable {

        private long mCount = 0; // tracking number of non-NA values
        private double mMean = 0; // tracking mean
        private double mSS = 0; // sum of squared deviations
        private long mNACount = 0; // tracking number of NA values
        private double mMin = Double.MAX_VALUE;
        private double mMax = Double.MIN_VALUE;

        public Summary() {
        }

        public Summary(double[] numbers) {
            this.merge(numbers);
        }

        public Summary(long mCount, double mMean, double mSS, long mNACount,
                       double mMin, double mMax) {
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
            if (mCount == 0)
                return Double.NaN;
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
            if (this.mCount == 0)
                return Double.NaN;
            return this.mMin;
        }

        public double max() {
            if (this.mCount == 0)
                return Double.NaN;
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
            return joiner.join("mean:", Utils.roundUp(mean()), " stdev:",
                    Utils.roundUp(stdev()), " var:", Utils.roundUp(variance()), " cNA:",
                    mNACount, " count:", mCount, " min:", Utils.roundUp(min()), " max:",
                    Utils.roundUp(max()));
        }
    }

    public class FiveNumSummary implements Serializable {

        private static final long serialVersionUID = 1L;
        private double mMin = 0;
        private double mMax = 0;
        private double mFirst_quantile = 0;
        private double mMedian = 0;
        private double mThird_quantile = 0;


        public FiveNumSummary() {

        }

        public FiveNumSummary(double mMin, double mMax, double first_quantile, double median, double third_quantile) {
            this.mMin = mMin;
            this.mMax = mMax;
            this.mFirst_quantile = first_quantile;
            this.mMedian = median;
            this.mThird_quantile = third_quantile;
        }

        public double getMin() {
            return mMin;
        }

        public void setMin(double mMin) {
            this.mMin = mMin;
        }

        public double getMax() {
            return mMax;
        }

        public void setMax(double mMax) {
            this.mMax = mMax;
        }

        public double getFirstQuantile() {
            return mFirst_quantile;
        }

        public void setFirstQuantile(double mFirst_quantile) {
            this.mFirst_quantile = mFirst_quantile;
        }

        public double getMedian() {
            return mMedian;
        }

        public void setMedian(double mMedian) {
            this.mMedian = mMedian;
        }

        public double getThirdQuantile() {
            return mThird_quantile;
        }

        public void setThirdQuantile(double mThird_quantile) {
            this.mThird_quantile = mThird_quantile;
        }

    }

    public abstract class SimpleSummary implements Serializable {

        private String mColumnName;

        public String getColumnName() {
            return this.mColumnName;
        }

        public void setColumnName(String colName) {
            this.mColumnName = colName;
        }
    }


}
 
