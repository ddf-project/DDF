package io.ddf2.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.HistogramBin;

import java.util.List;

public interface IBinningHandler extends IDDFHandler{

    /**
     * Compute factor of columns using histogram.
     * @param column The column name.
     * @param binningType Currently support: "EQUALINTERVAL" "EQUAlFREQ" "custom" // TODO: change to enum
     * @param numBins Num of factors. For factors, set numBins to 0.
     * @param breaks The list of break points, for example [2, 4, 6, 8]. Set this param to null if binningType is not
     *               custom.
     * @param includeLowest Whether to include the lowest value.
     * @param right Whether to include the right value.
     * @return
     * @throws DDFException
     */
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                       boolean right) throws DDFException;

    /**
     * Compute a histogram of the data using numBins buckets evenly spaced between the minimum and maximum.
     * @param column Column name.
     * @param numBins Num of buckets.
     * @return
     * @throws DDFException
     */
    public List<HistogramBin> getVectorHistogram(String column, int numBins) throws DDFException;

    public List<HistogramBin> getVectorApproxHistogram(String column, int numBins) throws DDFException;
 
}
 
