package io.ddf2.handlers.impl;

import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.FiveNumSummary;
import io.ddf2.analytics.SimpleSummary;
import io.ddf2.analytics.Summary;
import io.ddf2.handlers.IStatisticHandler;

import java.util.List;

public class StatisticHandler implements IStatisticHandler{
    protected IDDF associatedDDF;
    public StatisticHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public Summary[] getSummary() throws DDFException {
        return new Summary[0];
    }

    @Override
    public SimpleSummary[] getSimpleSummary() throws DDFException {
        return new SimpleSummary[0];
    }

    @Override
    public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException {
        return new FiveNumSummary[0];
    }

    @Override
    public double[] getQuantiles(String columnName, Double[] percentiles) throws DDFException {
        return new double[0];
    }

    @Override
    public double[] getVariance(String columnName) throws DDFException {
        return new double[0];
    }

    @Override
    public double getMean(String columnName) throws DDFException {
        return 0;
    }

    @Override
    public double getCor(String xColumnName, String yColumnName) throws DDFException {
        return 0;
    }

    @Override
    public double getCovariance(String xColumnName, String yColumnName) throws DDFException {
        return 0;
    }

    @Override
    public double getMin(String columnName) throws DDFException {
        return 0;
    }

    @Override
    public double getMax(String columnName) throws DDFException {
        return 0;
    }

    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}

