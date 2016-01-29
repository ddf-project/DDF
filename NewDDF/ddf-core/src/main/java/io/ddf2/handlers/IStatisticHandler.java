package io.ddf2.handlers;

import io.ddf2.DDFException;
import io.ddf2.analytics.FiveNumSummary;
import io.ddf2.analytics.SimpleSummary;
import io.ddf2.analytics.Summary;

import java.util.List;

public interface IStatisticHandler extends IDDFHandler {
    /**
     * Get summary for the ddf.
     * @return
     * @throws DDFException
     */
    Summary[] getSummary() throws DDFException;

    /**
     * Get min/max for numeric columns, list of distinct values for categorical columns
     * @return
     * @throws DDFException
     */
    SimpleSummary[] getSimpleSummary() throws DDFException;

    /**
     * Get five num (0, 0.25, 0.5, 0.75 and 1 percentile) summary
     * @return
     * @throws DDFException
     */
    FiveNumSummary[] getFiveNumSummary(List<String> columnNames) throws DDFException;

    /**
     * Get values at the given percentiles.
     * @param columnName The name of the column.
     * @param percentiles The percentiles.
     * @return
     * @throws DDFException
     */
    double[] getQuantiles(String columnName, Double[] percentiles) throws DDFException;

    /**
     * Get variance for the column data.
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    double[] getVariance(String columnName) throws DDFException;

    /**
     * Get mean value for the column name.
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    double getMean(String columnName) throws DDFException;

    /**
     * Get correlation between the two columns.
     * @param xColumnName The first column name.
     * @param yColumnName The second column name.
     * @return
     * @throws DDFException
     */
    double getCor(String xColumnName, String yColumnName) throws DDFException;

    /**
     * Get covariance between the two columns.
     * @param xColumnName The first column name.
     * @param yColumnName The second column name.
     * @return
     * @throws DDFException
     */
    double getCovariance(String xColumnName, String yColumnName) throws DDFException;

    /**
     * Get min value for the column.
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    double getMin(String columnName) throws DDFException;

    /**
     * Get max value for the column.
     * @param columnName The column name.
     * @return
     * @throws DDFException
     */
    double getMax(String columnName) throws DDFException;
}
 
