package io.ddf2.analytics;

/**
 * Created by echo on 2/5/16.
 */
public class NumericSimpleSummary extends SimpleSummary {
    private double min;
    private double max;

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return this.min;
    }

    public double getMax() {
        return this.max;
    }
}