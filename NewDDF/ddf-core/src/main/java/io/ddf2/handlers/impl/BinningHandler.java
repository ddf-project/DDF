package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.IBinningHandler;

import java.util.List;

public class BinningHandler implements io.ddf2.handlers.IBinningHandler {
    protected IDDF associatedDDF;
    public BinningHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
        return null;
    }

    @Override
    public IDDF binningCustom(String column, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
        return null;
    }

    @Override
    public IDDF binningEq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
        return null;
    }

    @Override
    public IDDF binningEqFreq(String column, int numBins, boolean includeLowest, boolean right) throws DDFException {
        return null;
    }

    @Override
    public List<HistogramBin> getHistogram(String column, int numBins) throws DDFException {
        return null;
    }

    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}

