package io.ddf2.handlers;


import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.analytics.AggregateTypes.*;
import java.util.List;

public interface IAggregationHandler extends IDDFHandler {
    /**
     * Compute corrlelation between 2 columns.
     * @param columnA The column name.
     * @param columnB The column name.
     * @return
     * @throws DDFException
     */
    public double computeCorrelation(String columnA, String columnB) throws DDFException;

    public AggregationResult aggregate(List<AggregateField> fields) throws DDFException;

    public AggregationResult xtabs(List<AggregateField> fields) throws DDFException;

    public IDDF groupBy(List<String> groupedColumns, List<String> aggregateFunctions) throws DDFException;

    public double aggregateOnColumn(AggregateFunction function, String col) throws DDFException;

    public IDDF agg(List<String> aggregateFunctions) throws DDFException;

    public IDDF groupBy(List<String> groupedColumns);

}
