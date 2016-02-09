package io.ddf2.handlers.impl;

import com.google.common.base.Joiner;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.schema.Factor;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.handlers.IViewHandler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ViewHandler implements IViewHandler{
    protected IDDF ddf;
    public ViewHandler(IDDF ddf){
        this.ddf = ddf;
    }
    @Override
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement) throws DDFException {
        return ddf.sql(buildRandomSampleSql(numSamples,withReplacement));
    }

    /**
     * Sample from ddf.
     *
     * @param numSamples      number of samples.
     * @param withReplacement Can elements be sampled multiple times.
     * @return DDF containing `numSamples` rows selected randomly from our owner DDF.
     */
    @Override
    public IDDF getRandomSample2(int numSamples, boolean withReplacement) throws DDFException {
        return ddf.sql2ddf(buildRandomSampleSql(numSamples, withReplacement));
    }


    @Override
    public IDDF getRandomSample(double percent, boolean withReplacement) throws DDFException {
        return ddf.sql2ddf(buildRandomSampleSql(percent, withReplacement));
    }



    @Override
    public ISqlResult head(int numRows) throws DDFException {
        String sqlCmd=String.format("SELECT * FROM %s LIMIT %d", ddf.getDDFName(),numRows);
        return ddf.sql(sqlCmd);
    }

    @Override
    public ISqlResult top(int numRows, String orderByCols, boolean isDesc) throws DDFException {
        String sorted = isDesc ? "DESC" : "ASC";
        String sqlCmd =String.format("SELECT * FROM %s order by %s %s", orderByCols, sorted);
        return ddf.sql(sqlCmd);
    }

    @Override
    public IDDF project(String... columnNames) throws DDFException {
        return project(Arrays.asList(columnNames));
    }

    @Override
    public IDDF project(List<String> columnNames) throws DDFException {
        String selectedColumns = Joiner.on(",").join(columnNames);
        IDDF projectedDDF =  ddf.sql2ddf(String.format("SELECT %s FROM %s", selectedColumns));
        // Copy the factor
        for (IColumn column : this.getDDF().getSchema().getColumns()) {
            if (projectedDDF.getSchema().getColumn(column.getName()) != null) {
                Factor factor = column.getFactor();
                if (factor != null) {
                    // TODO: copy factor here
                }
            }
        }
        return projectedDDF;
    }

    @Override
    public IDDF subset(List<Column> columnExpr, Expression filter) throws DDFException {
        return null;
    }

    @Override
    public IDDF subset(List<String> columnExpr, String filter) throws DDFException {
        return null;
    }

    @Override
    public IDDF removeColumn(String columnName) throws DDFException {
        return null;
    }

    @Override
    public IDDF removeColumns(String... columnNames) throws DDFException {
        return null;
    }

    @Override
    public IDDF removeColumns(List<String> columnNames) throws DDFException {
        return null;
    }

    @Override
    public IDDF getDDF() {
        return ddf;
    }

    /**
     * Simple solution by using select * from limit rand(),num
     * Each concrete class should override this method.
     * @param numSamples total row
     * @param withReplacement allow row to be duplicated
     * @return sampleSql
     */
    protected String buildRandomSampleSql(int numSamples,boolean withReplacement){
        assert numSamples > 0;
        long numRows = ddf.getNumRows();
        String sampleSql;
        if(numRows < numSamples){
            sampleSql="select * from " + ddf.getDDFName() + " limit " + numSamples;
        }else{
            long bound = numRows - numSamples;
            long pivot = ThreadLocalRandom.current().nextLong(0, bound);
            sampleSql = "select * from " + ddf.getDDFName() + " limit " + pivot + "," + numSamples;
        }
        return sampleSql;
    }
    /**
     * @param percent of total row range from: (0.0-1.0)
     * @param withReplacement allow row to be duplicated
     * @return sampleSql
     */
    protected String buildRandomSampleSql(double percent,boolean withReplacement){
        assert percent > 0 && percent < 1;
        long numRows = Math.round(ddf.getNumRows() * percent);
        return buildRandomSampleSql(numRows,withReplacement);
    }
}

