package io.ddf2.handlers;

import io.ddf2.DDFException;
import io.ddf2.IDDF;

import java.util.List;

public interface ITransformHandler extends IDDFHandler{
    public IDDF transformScaleMinMax() throws DDFException;

    public IDDF transformScaleStandard() throws DDFException;

    public IDDF transformNativeRserve(String transformExpression);

    public IDDF transformNativeRserve(String[] transformExpression);

    public IDDF transformPython(String[] transformFunctions, String[] functionNames,
                               String[] destColumns, String[][] sourceColumns);

    public IDDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);

    /**
     * Create new columns or overwrite existing ones
     *
     * @param transformExpressions A list of expressions, each is in format of column=expression or expression
     * @param columns
     * @return
     * @throws DDFException
     */
    public IDDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException;

    public IDDF flattenDDF(String[] columns) throws DDFException;

    public IDDF flattenDDF() throws DDFException;
}
 
