package io.ddf2.handlers.impl;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.datasource.schema.IColumn;
import io.ddf2.handlers.IStatisticHandler;
import io.ddf2.handlers.ITransformHandler;

import java.util.List;

public class TransformHandler implements ITransformHandler{
    protected IDDF associatedDDF;
    public TransformHandler(IDDF associatedDDF){
        this.associatedDDF = associatedDDF;
    }
    @Override
    public IDDF transformScaleMinMax() throws DDFException {
//        IStatisticHandler.Summary[] summaryArr = ddf.getStatisticHandler().getSummary();
//        List<IColumn> columns = this.getRandomSample2().getSchema().getColumns();
//
//        // Compose a transformation query
//        StringBuffer sqlCmdBuffer = new StringBuffer("SELECT ");
//
//        for (int i = 0; i < columns.size(); i++) {
//            Column col = columns.get(i);
//            if (!col.isNumeric() || col.getColumnClass() == ColumnClass.FACTOR) {
//                sqlCmdBuffer.append(col.getName()).append(" ");
//            } else {
//                // subtract min, divide by (max - min)
//                sqlCmdBuffer.append(String.format("((%s - %s) / %s) as %s ", col.getName(), summaryArr[i].min(),
//                        (summaryArr[i].max() - summaryArr[i].min()), col.getName()));
//            }
//            sqlCmdBuffer.append(",");
//        }
//        sqlCmdBuffer.setLength(sqlCmdBuffer.length() - 1);
//        sqlCmdBuffer.append("FROM ").append(this.getRandomSample2().getTableName());
//
//        DDF newddf = this.getManager().sql2ddf(sqlCmdBuffer.toString(), this.getEngine());
//        newddf.getMetaDataHandler().copyFactor(this.getRandomSample2());
//        return newddf;
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

