package io.ddf2.handlers;

import io.ddf2.DDFException;
import io.ddf2.IDDF;

import java.util.List;

public interface IBinningHandler extends IDDFHandler {

    @Deprecated
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                        boolean right) throws DDFException;


    public IDDF binningCustom(String column,double[] breaks, boolean includeLowest,boolean right) throws DDFException;

    public IDDF binningEq(String column,int numBin, boolean includeLowest,boolean right) throws DDFException;

    public IDDF binningEqFreq(String column,int numBin, boolean includeLowest,boolean right) throws DDFException;

    public List<HistogramBin> getHistogram(String column, int numBin) throws DDFException;



    public class HistogramBin {
        private double center; // Bin center
        private double weight; // Bin weight

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
 
