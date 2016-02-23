package io.ddf2.handlers;

import com.google.common.base.Joiner;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.Utils;

import java.io.Serializable;
import java.util.List;

public interface ITransformHandler extends IDDFHandler{
    /**
     * Transform the ddf so that all numeric column values are scaled based on the formula :
     * newVal = (value - minVal) / (maxVal - minVal)
     * @return
     * @throws DDFException
     */
    IDDF transformScaleMinMax() throws DDFException;

    /**
     * Transform the ddf so that all numeric columns values are scaled based on the formula
     * @return
     * @throws DDFException
     */
    IDDF transformScaleStandard() throws DDFException;

    IDDF transformNativeRserve(String transformExpression);

    IDDF transformNativeRserve(String[] transformExpression);

    IDDF transformPython(String[] transformFunctions, String[] functionNames,
                               String[] destColumns, String[][] sourceColumns);

    IDDF transformMapReduceNative(String mapFuncDef, String reduceFuncDef, boolean mapsideCombine);

    /**
     * Create new columns or overwrite existing ones.
     *
     * @param transformExpressions A list of expressions, each is in format of column=expression or expression, e.g.
     *                             "newColumnName = valueA * 2", then a new column named newColumnName will be
     *                             created with value equals to valueA * 2. "valueA * 2" will assign a name to the
     *                             new column by system.
     * @param columns Columns that needs to be projected.
     * @return
     * @throws DDFException
     */
    public IDDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException;

    /**
     * Flatten columns with structure to flat one, e.g. a column named a with properties val1, val2. Then new columns
     * a_val1, a_val2 will be created.
     * @param columns The columns to be flatted.
     * @return
     * @throws DDFException
     */
    IDDF flattenDDF(String[] columns) throws DDFException;

    /**
     * Flatten all columns with structure to flat one, e.g. a column named a with properties val1, val2. Then new
     * columns a_val1, a_val2 will be created.
     * @return
     * @throws DDFException
     */
    IDDF flattenDDF() throws DDFException;



}
 
