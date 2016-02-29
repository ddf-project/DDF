package io.ddf2.handlers.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import io.ddf2.DDFException;
import io.ddf2.DDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.datasource.schema.IFactor;
import io.ddf2.handlers.IViewHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ViewHandler implements IViewHandler{
    protected DDF ddf;
    public ViewHandler(DDF ddf){
        this.ddf = ddf;
    }
    @Override
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement) throws DDFException {
        return ddf.sql(buildRandomSampleSql(numSamples, withReplacement));
    }

    /**
     * Sample from ddf.
     *
     * @param numSamples      number of samples.
     * @param withReplacement Can elements be sampled multiple times.
     * @return DDF containing `numSamples` rows selected randomly from our owner DDF.
     */
    @Override
    public DDF getRandomSample2(int numSamples, boolean withReplacement) throws DDFException {
        return ddf.sql2ddf(buildRandomSampleSql(numSamples, withReplacement));
    }


    @Override
    public DDF getRandomSample(double percent, boolean withReplacement) throws DDFException {
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
    public DDF project(String... columnNames) throws DDFException {
        return project(Arrays.asList(columnNames));
    }

    @Override
    public DDF project(List<String> columnNames) throws DDFException {
        String selectedColumns = Joiner.on(",").join(columnNames);
        DDF projectedDDF =  ddf.sql2ddf(String.format("SELECT %s FROM %s", selectedColumns));
        // Copy the factor
        for (IColumn column : this.getDDF().getSchema().getColumns()) {
            if (projectedDDF.getSchema().getColumn(column.getName()) != null) {
                IFactor factor = column.getFactor();
                if (factor != null) {
                    projectedDDF.getSchema().setAsFactor(column.getName());
                    IFactor newFactor = projectedDDF.getSchema().getColumn(column.getName()).getFactor();
                    if (factor.getLevelCounts() != null) {
                        newFactor.setLevelCounts(factor.getLevelCounts());
                    }
                    if (factor.getLevels() != null) {
                        newFactor.setLevels(factor.getLevels());
                    }
                }
            }
        }
        return projectedDDF;
    }

    private void updateColumnName(Expression expression) throws DDFException {
        if (expression == null) return;
        if (expression instanceof Column) {
            Column column = (Column) expression;
            if (column.getName() == null) {
                Integer i = column.getIndex();
                if (i != null) {
                    column.setName(this.getDDF().getSchema().getColumnName(i));
                }
            }
            return;
        } else if (expression instanceof Operator){
            Expression[] exps = ((Operator) expression).getOperands();
            for (Expression exp : exps) {
                updateColumnName(exp);
            }
        }
    }

    @Override
    public DDF subset(List<Column> columnExprs, Expression filter) throws DDFException {
        this.updateColumnName(filter);
        List<String> columnStrs = new ArrayList<>();
        for (Column column : columnExprs) {
            this.updateColumnName(column);
            columnStrs.add(column.getName());
        }
        return this.subset(columnStrs, filter.toSql());
    }

    @Override
    public DDF subset(List<String> columnExprs, String filter) throws DDFException {
        String sqlcmd = String.format("SELECT %s FROM %s", Joiner.on(", ").join(columnExprs),
            this.getDDF().getDDFName());
        if (!Strings.isNullOrEmpty(filter)) {
            sqlcmd = String.format("%s WHERE %s", sqlcmd, filter);
        }
        return this.getDDF().sql2ddf(sqlcmd);
    }

    @Override
    public DDF removeColumn(String columnName) throws DDFException {
        assert !Strings.isNullOrEmpty(columnName);
        return this.removeColumns(Collections.singletonList(columnName));
    }

    @Override
    public DDF removeColumns(String... columnNames) throws DDFException {
        assert columnNames != null && columnNames.length > 0;
        return this.removeColumns(Arrays.asList(columnNames));
    }

    @Override
    public DDF removeColumns(List<String> columnNames) throws DDFException {
        assert columnNames != null && !columnNames.isEmpty();
        List<String> curColNames = this.getDDF().getSchema().getColumnNames();
        List<String> remainCols = new ArrayList<>(curColNames);
        for (String columnName : columnNames) {
            if (!curColNames.contains(columnName)) {
                throw new DDFException(String.format("Column name : %s doesn't exist in current ddf", columnName));
            }
            remainCols.remove(columnName);
        }
        DDF ddf = this.project(remainCols);
        // TODO: updateInPlace and copyFactor
        return ddf;
    }

    @Override
    public DDF getDDF() {
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

