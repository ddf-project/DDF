package io.ddf2.handlers.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.Utils;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.handlers.IAggregationHandler;
import org.apache.commons.lang.StringUtils;

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

        String sqlCmd = String.format("SELECT CORR(%s, %s) FROM %s", columnA, columnB, associatedDDF.getDDFName());
        ISqlResult sqlResult = associatedDDF.sql(sqlCmd);
        if (sqlResult.next())
            return Utils.roundUp(sqlResult.getDouble(0));
        else throw new DDFException("sqlresult don't contain expected data");
    }

    /**
     * Do aggregation.
     * eg: ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)")
     *
     * @param query contain group by column & query function
     * @return Aggregation Result
     * @throws DDFException
     */
    @Override
    public AggregationResult aggregate(String query) throws DDFException {
        assertAggregationQuery(query);
        String sqlCmd = "SELECT " + query + " from " + associatedDDF.getDDFName();
        ISqlResult sqlResult = associatedDDF.sql(sqlCmd);
        return new AggregationResult(sqlResult);
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
        String sqlCommand = String.format("SELECT %s from %s", function.toString(column), associatedDDF.getDDFName());
        ISqlResult sql = associatedDDF.sql(sqlCommand);
        if(sql.next()){
            return sql.getDouble(0);
        }else{
            throw new DDFException("Unable to get result from query " + sqlCommand);
        }
    }



    @Override
    public IDDF groupBy(List<String> columns, List<String> functions) throws DDFException {
        assert columns != null && columns.size() > 0;
        assert functions != null && functions.size() > 0;
        String groupedColSql = StringUtils.join(columns, ",");
        String selectFuncSql = convertAggregateFunctionsToSql(functions.get(0));
        for (int i = 1; i < functions.size(); i++) {
            selectFuncSql += "," + convertAggregateFunctionsToSql(functions.get(i));
        }

        String sqlCmd = String.format("SELECT %s , %s FROM %s GROUP BY %s",
                selectFuncSql,
                groupedColSql,
                associatedDDF.getDDFName(),
                groupedColSql);
        return associatedDDF.sql2ddf(sqlCmd);
    }

    @Override
    @Deprecated
    public AggregationResult xtabs(List<AggregateField> fields) throws DDFException {
        return aggregate(fields);
    }

    @Override
    @Deprecated
    public AggregationResult xtabs(String fields) throws DDFException {
        return aggregate(fields);
    }

    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }

    /**
     * Assert query is in right format.
     * The concrete class could override it to support certain Sql-Function
     * @param query eg: a,b,c,max(a),avg(b)
     * @throws DDFException
     */
    protected void assertAggregationQuery(String query) throws DDFException {
    }
    /**
     * Utility to check a column is numeric or not.
     *
     * @param columnA column name to check
     * @throws DDFException if column type is not numeric.
     * @see this.isNummeric(Class) for more information.
     */
    private void assertNumericType(String columnA) throws DDFException {
        if (associatedDDF.getSchema().getColumn(columnA).isNumeric() == false) {
            throw new DDFException(columnA + " must numberic datatype");
        }
    }
    /**
     * //ToDo: @Jing add sql example here for ussage.
     * @param sql
     * @return
     */
    @Deprecated
    protected String convertAggregateFunctionsToSql(String sql) {

        if (Strings.isNullOrEmpty(sql)) return null;

        String[] splits = sql.trim().split("=(?![^()]*+\\))");
        if (splits.length == 2) {
            return String.format("%s AS %s", splits[1], splits[0]);
        } else if (splits.length == 1) { // no name for aggregated value
            return splits[0];
        }
        return sql;
    }
    /**
     * Converts from a SQL String specs like "a, b, SUM(c), MIN(c)" into an array of SQL {@link AggregateField}s. This
     * is useful for constructing arguments to the aggregate function.
     *
     * @param sqlFieldSpecs
     * @return null if sqlFieldSpecs is null or empty
     */
    @Deprecated
    public static List<AggregateField> fromSqlFieldSpecs(String sqlFieldSpecs) {
        if (Strings.isNullOrEmpty(sqlFieldSpecs)) return null;

        String[] specs = sqlFieldSpecs.split(",");
        List<AggregateField> fields = Lists.newArrayList();
        for (String spec : specs) {
            if (Strings.isNullOrEmpty(spec)) continue;
            fields.add(fromFieldSpec(spec));
        }

        return fields;
    }
    @Deprecated
    public static AggregateField fromFieldSpec(String fieldSpec) {
        if (Strings.isNullOrEmpty(fieldSpec)) return null;

        String spec = fieldSpec.trim();
        String[] parts = spec.split("\\(");
        if (parts.length == 1) {
            return new AggregateField(parts[0]); // just column name
        } else {
            return new AggregateField(parts[0], parts[1].replaceAll("\\)", "")); // function(columnName)
        }
    }
}
