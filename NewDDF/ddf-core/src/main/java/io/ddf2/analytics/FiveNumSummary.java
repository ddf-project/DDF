package io.ddf2.analytics;

import java.io.Serializable;

/**
 * Created by jing on 1/25/16.
 */
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