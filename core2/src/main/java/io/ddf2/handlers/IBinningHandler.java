package io.ddf2.handlers;

import io.ddf2.DDFException;
import io.ddf2.DDF;

import java.util.List;

public interface IBinningHandler<T extends DDF<T>> extends IDDFHandler<T> {

    /**
     * Compute factor of columns using histogram.
     * @param column The column name.
     * @param binningType Currently support: "EQUALINTERVAL" "EQUAlFREQ" "custom"
     * @param numBins Num of factors. For factors, set numBins to 0.
     * @param breaks The list of break points, for example [2, 4, 6, 8]. Set this param to null if binningType is not
     *               custom.
     * @param includeLowest Whether to include the lowest value.
     * @param right Whether to include the right value.
     * @return
     * @throws DDFException
     */
    @Deprecated
    public T binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                        boolean right) throws DDFException;

    /**
     * Compute factor using histogram by defining break points.
     * @param column The column name.
     * @param breaks The break points.
     * @param includeLowest Whether to include the lowest value.
     * @param right Whether to include the right value.
     * @return
     * @throws DDFException
     */
    public T binningCustom(String column,double[] breaks, boolean includeLowest,boolean right) throws DDFException;

    /**
     * Compute factor using histogram by equal intervals.
     * @param column The column name.
     * @param numBins The number of buckets.
     * @param includeLowest Whether to include the lowest value.
     * @param right Whether to include the right value.
     * @return
     * @throws DDFException
     */
    public T binningEq(String column,int numBins, boolean includeLowest,boolean right) throws DDFException;

    /**
     * Compute factor using histogram by equal frequency.
     * @param column The column name.
     * @param numBins The number of buckets.
     * @param includeLowest Whether to include the lowest value.
     * @param right Whether to include the right value.
     * @return
     * @throws DDFException
     */
    public T binningEqFreq(String column,int numBins, boolean includeLowest,boolean right) throws DDFException;

    /**
     * Compute histogram of the column using numBins buckets, evenly spaced between the minimum and maximum value.
     * @param column The column name.
     * @param numBins Number of buckets.
     * @return
     * @throws DDFException
     */
    public List<HistogramBin> getHistogram(String column, int numBins) throws DDFException;

    public class HistogramBin {
        // Bin center
        private double center;
        // Bin weight
        private double weight;

        public HistogramBin(double center,double weight){
            this.center = center;
            this.weight = weight;
        }
        public double getCenter() {
            return center;
        }

        public double getWeight() {
            return weight;
        }
    }

    @Deprecated
    public enum BinningType {
        CUSTOM, EQUAlFREQ, EQUALINTERVAL;

        public static BinningType get(String s) {
            if (s == null || s.length() == 0) return null;

            for (BinningType type : values()) {
                if (type.name().equalsIgnoreCase(s)) return type;
            }

            return null;
        }
    }

}
 
