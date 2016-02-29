package io.ddf2.handlers.impl;

import io.ddf2.DDFException;
import io.ddf2.DDF;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.handlers.IStatisticHandler;
import io.ddf2.handlers.ITransformHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransformHandler implements ITransformHandler{
    protected DDF associatedDDF;
    public TransformHandler(DDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }

    @Override
    public DDF transformScaleMinMax() throws DDFException {
        return this.transformScaleImpl("minmax");
    }

    @Override
    public DDF transformScaleStandard() throws DDFException {
        return this.transformScaleImpl("standard");
    }

    protected DDF transformScaleImpl(String transformType) throws DDFException {
        assert Arrays.asList("minmax", "standard").contains(transformType.toLowerCase());

        IStatisticHandler.Summary[] summaryArr = this.getDDF().getStatisticHandler().getSummary();
        List<IColumn> columns = this.getDDF().getSchema().getColumns();

        StringBuffer sqlCmdBuffer = new StringBuffer("SELECT ");

        for (int i = 0; i < columns.size(); i++) {
            IColumn col = columns.get(i);
            if (!col.isNumeric() || col.getFactor() != null) {
                sqlCmdBuffer.append(col.getName()).append(" ");
            } else {
                // subtract min, divide by (max - min)
                sqlCmdBuffer.append(this.buildTransformScaleField("minmax",
                    summaryArr[i],
                    col.getName()));
            }
            sqlCmdBuffer.append(", ");
        }
        sqlCmdBuffer.setLength(sqlCmdBuffer.length() - 1);
        sqlCmdBuffer.append("FROM ").append(this.getDDF().getDDFName());

        DDF newddf = this.getDDF().sql2ddf(sqlCmdBuffer.toString());
        newddf.getSchema().copyFactor(this.getDDF());
        return newddf;
    }

    protected String buildTransformScaleField(String transformType, IStatisticHandler.Summary summary, String columnName) throws DDFException {
        assert Arrays.asList("minmax", "standard").contains(transformType.toLowerCase());
        if (transformType.equalsIgnoreCase("minmax")) {
            return String.format("((%s - %s) / %s) AS %s", columnName, summary.min(), summary.max() - summary.min(),
                columnName);
        } else if (transformType.equalsIgnoreCase("standard")) {
            return String.format("((%s - %s) / %s) AS %s", columnName, summary.mean(), summary.stdev() - summary.mean(),
                columnName);
        }
        throw new DDFException("Unsupported transform operation");
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

    @Override
    public DDF transformUDF(List<String> transformExpressions, List<String> columns) throws DDFException {
        List<String> curCols = new ArrayList<>();
        String sqlcmd = String.format("SELECT %s FROM %s",
            this.RToSqlUDF(transformExpressions, columns, this.getDDF().getSchema().getColumnNames()));
        DDF newddf = this.getDDF().sql2ddf(sqlcmd);
        // TODO factor
        return newddf;
    }

    @Override
    public DDF flattenDDF(String[] columns) throws DDFException {
        return null;
    }

    @Override
    public DDF flattenDDF() throws DDFException {
        return null;
    }

    @Override
    public DDF getDDF() {
        return associatedDDF;
    }

    public static String RToSqlUDF(List<String> transformExpressions, List<String> selectedCols, List<String> curCols) {
        List<String> udfs = new ArrayList<>();
        Map<String, String> newColToDef = new HashMap<String, String>();
        boolean updateOnConflict = (selectedCols == null || selectedCols.isEmpty());
        String dupColExp = "%s duplicates another column name";

        if (updateOnConflict) {
            if (curCols != null && !curCols.isEmpty()) {
                for (String col : curCols) {
                    udfs.add(col);
                }
            }
        }
        else {
            for (String c : selectedCols) {
                udfs.add(c);
            }
        }

        Set<String> newColsInRExp = new HashSet<String>();
        for (String str : transformExpressions) {
            int index = str.indexOf("=") >  str.indexOf("~")
                ? str.indexOf("=")
                : str.indexOf("~");
            String[] udf = new String[2];
            if (index == -1) {
                udf[0] = str;
            } else {
                udf[0] = str.substring(0,index);
                udf[1] = str.substring(index + 1);
            }

            // String[] udf = str.split("[=~](?![^()]*+\\))");
            String newCol = (index != -1) ?
                udf[0].trim().replaceAll("\\W", "") :
                udf[0].trim();
            if (newColsInRExp.contains(newCol)) {
                throw new RuntimeException(String.format(dupColExp, newCol));
            }
            String newDef = (index != -1) ? udf[1].trim() : null;
            if (!udfs.contains(newCol)) {
                udfs.add(newCol);
            }
            else if (!updateOnConflict) {
                throw new RuntimeException(String.format(dupColExp,newCol));
            }

            if (newDef != null && !newDef.isEmpty()) {
                newColToDef.put(newCol.replaceAll("\\W", ""), newDef);
            }
            newColsInRExp.add(newCol);
        }

        String selectStr = "";
        for (String udf : udfs) {
            String exp = newColToDef.containsKey(udf) ?
                String.format("%s as %s", newColToDef.get(udf), udf) :
                String.format("%s", udf);
            selectStr += (exp + ",");
        }

        return selectStr.substring(0, selectStr.length() - 1);
    }
}

