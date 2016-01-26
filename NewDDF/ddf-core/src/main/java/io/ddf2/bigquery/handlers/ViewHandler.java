package io.ddf2.bigquery.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.handlers.IDDFHandler;
import io.ddf2.handlers.IViewHandler;

import java.util.List;

public class ViewHandler implements IViewHandler{

    @Override
    public IDDF getDDF() {
        return null;
    }

    /**
     * @param numSamples
     * @param withReplacement
     * @param seed            @return a new DDF containing `numSamples` rows selected randomly from our owner DDF.
     */
    @Override
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public DDF getRandomSampleByNum(int numSamples, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public ISqlResult head(int numRows) throws DDFException {
        return null;
    }

    @Override
    public ISqlResult top(int numRows, String orderCols, String mode) throws DDFException {
        return null;
    }

    @Override
    public DDF project(String... columnNames) throws DDFException {
        return null;
    }

    @Override
    public DDF project(List<String> columnNames) throws DDFException {
        return null;
    }

    @Override
    public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException {
        return null;
    }

    @Override
    public DDF removeColumn(String columnName) throws DDFException {
        return null;
    }

    @Override
    public DDF removeColumns(String... columnNames) throws DDFException {
        return null;
    }

    @Override
    public DDF removeColumns(List<String> columnNames) throws DDFException {
        return null;
    }
}
 
