package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.handlers.IDDFHandler;
import io.ddf2.handlers.IViewHandler;

import java.util.List;

public class ViewHandler implements IViewHandler{
    protected IDDF associatedDDF;
    public ViewHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public IDDF getRandomSampleByNum(int numSamples, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public IDDF getRandomSample(double percent, boolean withReplacement, int seed) {
        return null;
    }

    @Override
    public ISqlResult head(int numRows) throws DDFException {
        return null;
    }

    @Override
    public ISqlResult top(int numRows, String orderByCols, boolean isDesc) throws DDFException {
        return null;
    }

    @Override
    public IDDF project(String... columnNames) throws DDFException {
        return null;
    }

    @Override
    public IDDF project(List<String> columnNames) throws DDFException {
        return null;
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
        return associatedDDF;
    }
}

