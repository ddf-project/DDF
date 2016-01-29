package io.ddf2.bigquery.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.HistogramBin;

import java.util.List;

public class BinningHandler implements io.ddf2.handlers.IBinningHandler {

    @Override
    public IDDF getDDF() {
        return null;
    }

    @Override
    public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
        return null;
    }

    @Override
    public List<HistogramBin> getVectorHistogram(String column, int numBins) throws DDFException {
        return null;
    }


}
 
