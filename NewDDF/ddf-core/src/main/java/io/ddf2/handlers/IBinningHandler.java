package io.ddf2.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.HistogramBin;

import java.util.List;

public interface IBinningHandler extends IDDFHandler{

    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest,
                       boolean right) throws DDFException;

    public List<HistogramBin> getVectorHistogram(String column, int numBins) throws DDFException;
    public List<HistogramBin> getVectorApproxHistogram(String column, int numBins) throws DDFException;
 
}
 
