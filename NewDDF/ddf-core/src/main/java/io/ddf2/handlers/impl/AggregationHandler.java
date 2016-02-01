package io.ddf2.handlers.impl;

import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.Utils;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.handlers.IAggregationHandler;

import java.util.List;

/**
 * Created by sangdn on 1/24/16.
 */
public abstract class AggregationHandler implements io.ddf2.handlers.IAggregationHandler {

    protected IDDF associatedDDF;

    public AggregationHandler(IDDF associatedDDF) {
        this.associatedDDF = associatedDDF;
    }

    @Override
    public double computeCorrelation(String columnA, String columnB) throws DDFException {
        assertNumericType(columnA);
        assertNumericType(columnB);

        String sqlCmd = String.format("SELECT CORR(%s, %s) FROM %s", columnA, columnB, this.getDDF().getDDFName());
        ISqlResult sqlResult = associatedDDF.sql(sqlCmd);
        if (sqlResult.next())
            return Utils.roundUp(sqlResult.getDouble(0));
        else throw new DDFException("sqlresult don't contain expected data");
    }


    @Override
    public AggregationResult aggregate(String query) throws DDFException {
        return null;
    }

    @Override
    public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {


        String sqlCmd = AggregateField.toSql(fields, associatedDDF.getDDFName());
        int numUnaggregatedFields = 0;

        for (AggregateField field : fields) {
            if (!field.isAggregated()) numUnaggregatedFields++;
        }

        ISqlResult result = associatedDDF.sql(sqlCmd);
        return new AggregationResult(result, numUnaggregatedFields);

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
        return associatedDDF;
    }

    /**
     * Utility to check a column is numeric or not.
     *
     * @param columnA column name to check
     * @throws DDFException if column type is not numeric.
     * @see this.isNummeric(Class) for more information.
     */
    private void assertNumericType(String columnA) throws DDFException {
        Class type = getDDF().getSchema().getColumn(columnA).getType();
        if (isNummeric(type) == false) {
            throw new DDFException(columnA + " must numberic datatype, current datatype is " + type.toString());
        }
    }

    protected abstract boolean isNummeric(Class type);
}
