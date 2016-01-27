package io.ddf2.handlers;

import io.ddf2.DDFException;
import io.ddf2.analytics.FiveNumSummary;
import io.ddf2.analytics.SimpleSummary;
import io.ddf2.analytics.Summary;

import java.util.List;

public interface IStatisticHandler extends IDDFHandler {
    public Summary[] getSummary() throws DDFException;

    //get min/max for numeric columns, list of distinct values for categorical columns
    public SimpleSummary[] getSimpleSummary() throws DDFException;

    public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

    public Double[] getVectorQuantiles(Double[] percentiles) throws DDFException;

    public Double[] getVectorQuantiles(String columnName, Double[] percentiles) throws DDFException;

    public Double[] getVectorVariance(String columnName) throws DDFException;

    public Double getVectorMean(String columnName) throws DDFException;

    public double getVectorCor(String xColumnName, String yColumnName) throws DDFException;

    public double getVectorCovariance(String xColumnName, String yColumnName) throws DDFException;

    public Double getVectorMin(String columnName) throws DDFException;

    public Double getVectorMax(String columnName) throws DDFException;
}
 
