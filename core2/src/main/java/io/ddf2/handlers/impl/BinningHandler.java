package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.DDF;
import io.ddf2.handlers.IBinningHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class BinningHandler implements io.ddf2.handlers.IBinningHandler {
    protected DDF associatedDDF;

    public BinningHandler(DDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }

    @Override
    public DDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
        assert Arrays.asList("custom", "equalInterval", "equalfreq").contains(binningType.toLowerCase());
        if (binningType.equalsIgnoreCase("custom")) {
            return this.binningCustom(column, breaks, includeLowest, right);
        } else if (binningType.equalsIgnoreCase("equalinterval")) {
            return this.binningEq(column, numBins, includeLowest, right);
        } else if (binningType.equalsIgnoreCase("equalfreq")) {
            return this.binningEqFreq(column, numBins, includeLowest, right);
        }
        throw new DDFException(String.format("Unsupported binning type: %s", binningType));
    }

    @Override
    public DDF getDDF() {
        return associatedDDF;
    }
}

