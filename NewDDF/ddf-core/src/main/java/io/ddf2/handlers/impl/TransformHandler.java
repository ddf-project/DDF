package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.ITransformHandler;

import java.util.List;

public class TransformHandler implements ITransformHandler{
    protected IDDF associatedDDF;
    public TransformHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public IDDF transformScaleMinMax() throws DDFException {
        return null;
    }

    @Override
    public IDDF transformScaleStandard() throws DDFException {
        return null;
    }

    @Override
    public IDDF transformNativeRserve(String transformExpression) {
        return null;
    }

    @Override
    public IDDF transformNativeRserve(String[] transformExpression) {
        return null;
    }

    @Override
    public IDDF transformPython(String[] transformFunctions, String[] functionNames, String[] destColumns, String[][] sourceColumns) {
        return null;
    }

    @Override
    public IDDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
        return null;
    }

    @Override
    public IDDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException {
        return null;
    }

    @Override
    public IDDF flattenDDF(String[] columns) throws DDFException {
        return null;
    }

    @Override
    public IDDF flattenDDF() throws DDFException {
        return null;
    }

    @Override
    public IDDF getDDF() {
        return associatedDDF;
    }
}

