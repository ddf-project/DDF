package io.ddf2.datasource.schema;

import io.ddf2.datasource.fileformat.resolver.TypeResolver;
import io.ddf2.datasource.fileformat.resolver.UnsupportedTypeException;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by sangdn on 1/4/16.
 * <p>
 * Schema keeps list of IColumn in order.
 */
@NotThreadSafe
public class Schema implements ISchema {


    protected Set<String> colNames;
    protected List<IColumn> columns;

    public Schema() {
        colNames = new HashSet<>();
        columns = new ArrayList<>();

    }

    public Schema(Schema schema) {
        this();
        this.colNames.addAll(schema.colNames);
        columns.addAll(schema.getColumns());
        assert colNames.size() == columns.size();
    }


    @Override
    public int getNumColumn() {
        return columns.size();
    }

    @Override
    public List<IColumn> getColumns() {
        return columns;
    }

    public void append(IColumn column) {
        if (colNames.add(column.getName()))
            columns.add(column);

    }

    public void remove(int index) {
        assert index >= 0 && index < columns.size();
        IColumn column = columns.remove(index);
        colNames.remove(column.getName());
    }

    public void remove(String colName) {
        if (colNames.contains(colName)) {
            columns.removeIf(a -> a.getName() == colName);
            colNames.remove(colName);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(IColumn column : columns){
            sb.append(column.getName() + ":" + column.getType().toString() + "|");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }

    public class Builder {
        protected Schema schema;

        public Builder() {
            schema = new Schema();
        }

        /**
         * @param nameAndType A column name with type.
         *                    Type Support: @see Schema.ReserveType
         *                    example. Builder.add("username string").add("age int")
         * @return
         */
        public Builder add(String nameAndType) throws UnsupportedTypeException {
            String[] nameAndTypes = nameAndType.split(" ");
            if (nameAndTypes != null && nameAndTypes.length == 2) {
                add(nameAndTypes[0], TypeResolver.getType(nameAndTypes[1]));
            } else {
                throw new IllegalArgumentException("Wrong Format Expect: ColumnName Type");
            }
            return this;
        }

        public Builder add(String colName, Class colType) {
            schema.append(new Column(colName, colType));
            return this;
        }
    }
}
