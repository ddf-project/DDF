package io.ddf2.bigquery.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.handlers.ITransformHandler;

import java.util.List;

public class TransformHandler implements ITransformHandler{

    @Override
    public IDDF getDDF() {
        return null;
    }

    @Override
    public DDF transformScaleMinMax() throws DDFException {
        return null;
    }

    @Override
    public DDF transformScaleStandard() throws DDFException {
        return null;
    }

    @Override
    public DDF transformNativeRserve(String transformExpression) {
        return null;
    }

    @Override
    public DDF transformNativeRserve(String[] transformExpression) {
        return null;
    }

    @Override
    public DDF transformPython(String[] transformFunctions, String[] functionNames, String[] destColumns, String[][] sourceColumns) {
        return null;
    }

    @Override
    public DDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine) {
        return null;
    }

    /**
     * Create new columns or overwrite existing ones
     *
     * @param transformExpressions A list of expressions, each is in format of column=expression or expression
     * @param columns
     * @return
     * @throws DDFException
     */
    @Override
    public DDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException {
        return null;
    }

    @Override
    public DDF flattenDDF(String[] columns) throws DDFException {
        return null;
    }

    @Override
    public DDF flattenDDF() throws DDFException {
        return null;
    }
}
 
