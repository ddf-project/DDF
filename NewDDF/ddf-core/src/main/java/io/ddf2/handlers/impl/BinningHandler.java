package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.IBinningHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class BinningHandler implements io.ddf2.handlers.IBinningHandler {
    protected IDDF associatedDDF;

    public BinningHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }

    @Override
    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
        assert Arrays.asList("custom", "equalInterval", "equalfreq").contains(binningType.toLowerCase());
        if (binningType.equalsIgnoreCase("custom")) {
            return this.binningCustom(column, breaks, includeLowest, right);
        } else if (binningType.equalsIgnoreCase("equalinterval")) {
            return this.binningEq(column, numBins, includeLowest, right);
        } else if (binningType.equalsIgnoreCase("equalfreq")) {
            return this.binningEqFreq(column, numBins, includeLowest, right);
        }
        // TODO: return null here?
        return null;
    }

    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}

