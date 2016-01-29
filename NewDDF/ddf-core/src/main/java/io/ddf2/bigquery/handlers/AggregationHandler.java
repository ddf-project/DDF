package io.ddf2.bigquery.handlers;

import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.IAggregationHandler;

import java.util.List;

/**
 * Created by sangdn on 1/24/16.
 */
public class AggregationHandler implements io.ddf2.handlers.IAggregationHandler{

    /**
     * Compute corrlelation between 2 numeric columns.
     *
     * @param columnA The column name.
     * @param columnB The column name.
     * @return
     * @throws DDFException
     */
    @Override
    public double computeCorrelation(String columnA, String columnB) throws DDFException {
        return 0;
    }

    /**
     * Do aggregated
     * eg: ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)")
     *
     * @param query contain group by column & query function
     * @return Aggregation Result
     * @throws DDFException
     */
    @Override
    public AggregationResult aggregate(String query) throws DDFException {
        return null;
    }

    @Override
    public IDDF aggregate(List<String> columns, List<String> functions) throws DDFException {
        return null;
    }

    @Override
    public double aggregate(String column, AggregateFunction function) throws DDFException {
        return 0;
    }

    /**
     * this function need to called after aggregate so we shouldn't use it.
     * use Aggregation with groupedColumns instead @see aggregate
     *
     * @param aggregateFunctions
     * @return
     * @throws DDFException
     */
    @Override
    public IDDF agg(List<String> aggregateFunctions) throws DDFException {
        return null;
    }

    @Override
    public IDDF aggregate(List<String> groupedColumns) {
        return null;
    }

    @Override
    public AggregationResult xtabs(List<AggregateField> fields) throws DDFException {
        return null;
    }

    @Override
    public AggregationResult xtabs(String fields) throws DDFException {
        return null;
    }

    /**
     * Every Handler have its associate ddf
     *
     * @return Associated DDF
     */
    @Override
    public IDDF getDDF() {
        return null;
    }
}
