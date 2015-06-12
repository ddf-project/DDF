package io.ddf.analytics;


import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;
import io.ddf.exception.DDFException;
import io.ddf.misc.IHandleDDFFunctionalGroup;

import java.util.List;

public interface ISupportStatistics extends IHandleDDFFunctionalGroup {

  public Summary[] getSummary() throws DDFException;

  public FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

  // public Double[] getVectorQuantiles(Double[] percentiles) throws DDFException;

  public Double[] getVectorQuantiles(String columnName, Double[] percentiles) throws DDFException;

  public Double[] getVectorVariance(String columnName) throws DDFException;

  public Double getVectorMean(String columnName) throws DDFException;

  public double getVectorCor(String xColumnName, String yColumnName) throws DDFException;

  public double getVectorCovariance(String xColumnName, String yColumnName) throws DDFException;

  public Double getVectorMin(String columnName) throws DDFException;

  public Double getVectorMax(String columnName) throws DDFException;
}
