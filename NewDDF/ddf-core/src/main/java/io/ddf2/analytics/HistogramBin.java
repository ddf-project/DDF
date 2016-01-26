package io.ddf2.analytics;

/**
 * Created by jing on 1/25/16.
 */
public class HistogramBin {
    private double x; // Bin center
    private double y; // Bin weight

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }
}

