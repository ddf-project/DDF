package io.ddf2.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.HistogramBin;

import java.util.List;

public interface IBinningHandler extends IDDFHandler{

    @Deprecated
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                       boolean right) throws DDFException;

    public IDDF binningCustom(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                        boolean right) throws DDFException;
    public IDDF binningEQ(String column, String binningType/*equal*/, int numBins,boolean includeLowest,
                        boolean right) throws DDFException;
    public IDDF binningEQFREQ(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                        boolean right) throws DDFException;

    public List<HistogramBin> getVectorHistogram(String column, int numBins) throws DDFException;

//    public List<HistogramBin> getVectorApproxHistogram(String column, int numBins) throws DDFException;

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
 
