package io.ddf2.handlers;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.schema.IColumn;

import java.io.Serializable;
import java.util.*;

/**
 * Created by jing on 1/25/16.
 */
public class ViewHandler implements IViewHandler {

    /*
    public ViewHandler(DDF theDDF) {
        super(theDDF);
    }
    */

    @Override
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement, int seed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DDF getRandomSampleByNum(int numSamples, boolean withReplacement,
                                    int seed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
        throw new UnsupportedOperationException();
    }


    @Override
    public ISqlResult head(int numRows) throws DDFException {
        return this.getDDF().sql(String.format("SELECT * FROM @this LIMIT %d", numRows));
    }

    public ISqlResult top(int numRows, String orderColumns, String mode) throws DDFException {

        /*
        DDF temp = sql2ddf(String.format("SELECT * FROM %%s order by %s %s", orderColumns, mode),
            String.format("Unable to fetch %d row(s) from table %%s", numRows));

        return (temp.VIEWS.head(numRows));*/
        return null;
    }

    @Override
    public DDF project(String... columnNames) throws DDFException {
        if (columnNames == null || columnNames.length == 0) throw new DDFException("columnNames must be specified");
        List<String> colNames = new ArrayList<String>(columnNames.length);
        for(String col: columnNames) {colNames.add(col);}
        return project(colNames);
    }

    @Override
    public DDF project(List<String> columnNames) throws DDFException {
        if (columnNames == null || columnNames.isEmpty()) throw new DDFException("columnNames must be specified");

        String selectedColumns = Joiner.on(",").join(columnNames);
        DDF projectedDDF = null;
            /*
            sql2ddf(String.format("SELECT %s FROM %%s", selectedColumns),
            String.format("Unable to project column(s) %s from table %%s", selectedColumns));*/

        //reserve factor information
        List<IColumn> columns = this.getDDF().getSchema().getColumns();
        for(IColumn column: columns) {
            if(projectedDDF.getSchema().getColumn(column.getName()) != null) {
                // TODO: handle factor here
                /*
                Factor<?> factor = column.getOptionalFactor();
                if(factor != null) {
                    mLog.info(">>> set factor for column " + column.getName());
                    projectedDDF.getSchemaHandler().setAsFactor(column.getName());
                    Factor<?> newFactor = projectedDDF.getSchema().getColumn(column.getName()).getOptionalFactor();
                    if(factor.getLevelCounts() != null) {newFactor.setLevelCounts(factor.getLevelCounts());}
                    if(factor.getLevels() != null) {newFactor.setLevels(factor.getLevels());}
                }
                */
            }
        }
        return projectedDDF;
    }


    @Override
    public DDF removeColumn(String columnName) throws DDFException {
        List<String> columns = Lists.newArrayList();

        Collections.addAll(columns, columnName);

        return this.removeColumns(columns);
    }

    @Override
    public DDF removeColumns(String... columnNames) throws DDFException {
        if (columnNames == null || columnNames.length == 0) throw new DDFException("columnNames must be specified");

        List<String> columns = Lists.newArrayList();

        Collections.addAll(columns, columnNames);

        return this.removeColumns(columns);
    }

    @Override
    public DDF removeColumns(List<String> columnNames) throws DDFException {
        if (columnNames == null || columnNames.isEmpty()) throw new DDFException("columnNames must be specified");
        List<String> currentColumnNames = this.getDDF().getSchema().getColumnNames();
        for(String columnName: columnNames) {
            if(!currentColumnNames.contains(columnName)) {
                throw new DDFException(String.format("Column %s does not exists", columnName));
            }
        }
        List<String> columns = this.getDDF().getSchema().getColumnNames();

        for (String columnName : columnNames) {
            for (Iterator<String> it = columns.iterator();it.hasNext();) {
                if (it.next().equals(columnName)) {
                    it.remove();
                }
            }
        }

        DDF newddf = this.project(columns);
        // TODO: mutable behavior?
        /*
        if(this.getDDF().isMutable()) {
            this.getDDF().updateInplace(newddf);
            return this.getDDF();
        } else {
            newddf.getMetaDataHandler().copyFactor(this.getDDF());
            return newddf;
        }
        */
        return newddf;
    }


    @Override
    public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException {
        DDF subset = _subset(columnExpr, filter);
        // subset.getMetaDataHandler().copyFactor(this.getDDF(), this.getDDF().getColumnNames());
        return subset;

    }

    protected DDF _subset(List<Column> columnExpr, Expression filter) throws DDFException {
        /*
        updateVectorName(filter, this.getDDF());
        mLog.info("Updated filter: " + filter);

        String[] colNames = new String[columnExpr.size()];
        for (int i = 0; i < columnExpr.size(); i++) {
            updateVectorName(columnExpr.get(i), this.getDDF());
            colNames[i] = columnExpr.get(i).getName();
        }
        mLog.info("Updated columns: " + Arrays.toString(columnExpr.toArray()));

        String sqlCmd = String.format("SELECT %s FROM %s", Joiner.on(", ").join
            (colNames), this.getDDF().getTableName());
        if (filter != null) {
            sqlCmd = String.format("%s WHERE %s", sqlCmd, filter.toSql());
        }
        mLog.info("sql = {}", sqlCmd);

        DDF subset = this.getDDF().getSqlHandler().sql2ddf(sqlCmd);
        return subset;
        */
        return null;
    }

    @Override
    public IDDF getDDF() {
        return null;
    }

    private IColumn[] selectColumnMetaInfo(List<Column> columns, DDF ddf) {
        int length = columns.size();
        IColumn[] retObj = new IColumn[length];
        /*
        for (int i = 0; i < length; i++) {
            retObj[i] = ddf.getSchema().getColumn(columns.get(i).getIndex());
        }
        */
        return retObj;
    }

    private void updateVectorIndex(Expression expression, DDF ddf) throws DDFException {
        if (expression == null) {
            return;
        }
        if (expression.getType().equals("Column")) {
            Column vec = (Column) expression;
            if (vec.getIndex() == null) {
                String name = vec.getName();
                if (name != null) {
                    vec.setIndex(ddf.getSchema().getColumnIndex(name));
                }
            }
            return;
        }
        if (expression instanceof Operator) {
            Expression[] newOps = ((Operator) expression).getOperands();
            for (Expression newOp : newOps) {
                updateVectorIndex(newOp, ddf);
            }
        }
    }

    protected void updateVectorName(Expression expression, DDF ddf) throws DDFException {
        if (expression == null) {
            return;
        }
        if (expression.getType().equals("Column")) {
            Column vec = (Column) expression;
            if (vec.getName() == null) {
                Integer i = vec.getIndex();
                if (i != null) {
                    vec.setName(ddf.getSchema().getColumnName(i));
                }
            }
            return;
        }
        if (expression instanceof Operator) {
            Expression[] newOps = ((Operator) expression).getOperands();
            for (Expression newOp : newOps) {
                updateVectorName(newOp, ddf);
            }
        }
    }
}
