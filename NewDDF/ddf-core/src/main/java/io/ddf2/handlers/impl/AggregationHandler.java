package io.ddf2.handlers.impl;

import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.IAggregationHandler;

import java.util.List;

/**
 * Created by sangdn on 1/24/16.
 */
public class AggregationHandler implements io.ddf2.handlers.IAggregationHandler{

    @Override
    public double computeCorrelation(String columnA, String columnB) throws DDFException {
        return 0;
    }

    @Override
    public AggregationResult aggregate(String query) throws DDFException {
        return null;
    }

    @Override
    public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {
        return null;
    }

    @Override
    public double aggregate(String column, AggregateFunction function) throws DDFException {
        return 0;
    }

    @Override
    public IDDF agg(List<String> aggregateFunctions) throws DDFException {
        return null;
    }

    @Override
    public IDDF groupBy(List<String> groupedColumns) {
        return null;
    }

    @Override
    public IDDF groupBy(List<String> columns, List<String> functions) throws DDFException {
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

    @Override
    public IDDF getDDF() {
        return null;
    }
}
