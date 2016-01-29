package io.ddf2.datasource.schema;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Created by sangdn on 1/4/16.
 */
@ThreadSafe
public class Column implements IColumn {
    protected String colName;
    protected Class dataType;
    protected IFactor factor;
    public Column(String colName, Class dataType){
        this.colName = colName;
        this.dataType = dataType;
    }
    public Column(String colName, Class dataType,IFactor factor){
        this.colName = colName;
        this.dataType = dataType;
    }
    @Override
    public String getName() {
        return colName;
    }

    @Override
    public Class getType() {
        return dataType;
    }

    @Override
    public IFactor getFactor() {
        return factor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Column column = (Column) o;
        return colName.equals(column.getName());
    }

    @Override
    public int hashCode() {
        return colName.hashCode();
    }
}
