package io.ddf2.bigquery.handlers;

import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.FiveNumSummary;
import io.ddf2.analytics.SimpleSummary;
import io.ddf2.analytics.Summary;
import io.ddf2.handlers.IStatisticHandler;

import java.util.List;

public class StatisticHandler implements IStatisticHandler{

    @Override
    public IDDF getDDF() {
        return null;
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
    public Double[] getVectorQuantiles(Double[] percentiles) throws DDFException {
        return new Double[0];
    }

    @Override
    public Double[] getVectorQuantiles(String columnName, Double[] percentiles) throws DDFException {
        return new Double[0];
    }

    @Override
    public Double[] getVectorVariance(String columnName) throws DDFException {
        return new Double[0];
    }

    @Override
    public Double getVectorMean(String columnName) throws DDFException {
        return null;
    }

    @Override
    public double getVectorCor(String xColumnName, String yColumnName) throws DDFException {
        return 0;
    }

    @Override
    public double getVectorCovariance(String xColumnName, String yColumnName) throws DDFException {
        return 0;
    }

    @Override
    public Double getVectorMin(String columnName) throws DDFException {
        return null;
    }

    @Override
    public Double getVectorMax(String columnName) throws DDFException {
        return null;
    }
}
 
