package io.ddf2.datasource.schema;

import io.ddf2.DDFException;
import io.ddf2.IDDF;

import org.apache.http.annotation.NotThreadSafe;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Created by sangdn on 1/4/16.
 */
@NotThreadSafe
public  class Schema implements ISchema {



    protected List<String> colNames;
    protected List<IColumn> columns;
    // TODO: @sang I think we need an inverse reference to the ddf?
    private IDDF associatedDDF;

    public Schema() {
        colNames = new ArrayList<>();
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

    @Override
    public IColumn getColumn(String columnName) throws DDFException {
        for (IColumn col : this.getColumns()) {
            if (col.getName().equalsIgnoreCase(columnName)) {
                return col;
            }
        }
        throw new DDFException(String.format("No column with column name: %s", columnName));
    }

    @Override
    public List<String> getColumnNames() {
        return this.colNames;
    }

    @Override
    public String getColumnName(int index) {
        return this.getColumns().get(index).getName();
    }

    @Override
    public int getColumnIndex(String columnName) throws DDFException {
        return this.getColumnNames().indexOf(columnName);
    }

    // TODO: @sang the schema class should be abstract and we should remove these 2 overrides later
    @Override
    public void computeFactorLevelsAndLevelCounts() throws DDFException {

    }

    @Override
    public void setFactorLevelsForStringColumns(String[] xCols) throws DDFException {

    }

    @Override
    public IFactor setAsFactor(String columnName) throws DDFException {
        IFactor factor = null;
        IColumn column  = this.getColumn(columnName);
        if (column == null) {
            throw new DDFException(String.format("Column : %s doesn't exist", columnName));
        }

        Class columnType = column.getType();
        if (columnType.equals(Double.class)) {
            factor = new Factor<Double>(this.associatedDDF, columnName);
        } else if (columnType.equals(Float.class)) {
            factor = new Factor<Float>(this.associatedDDF, columnName);
        } else if (columnType.equals(Integer.class)) {
            factor = new Factor<Integer>(this.associatedDDF, columnName);
        } else if (columnType.equals(Long.class)) {
            factor = new Factor<Long>(this.associatedDDF, columnName);
        } else if (columnType.equals(Boolean.class)) {
            factor = new Factor<Boolean>(this.associatedDDF, columnName);
        } else if (columnType.equals(String.class)) {
            factor = new Factor<String>(this.associatedDDF, columnName);
        } else if (columnType.equals(Timestamp.class)) {
            factor = new Factor<Timestamp>(this.associatedDDF, columnName);
        }

        column.setAsFactor(factor);
        return factor;
    }

    @Override
    public IFactor setAsFactor(int columnIndex) throws DDFException {
        String columnName = this.getColumnName(columnIndex);
        return this.setAsFactor(columnName);
    }

    @Override
    public void unsetAsFactor(String columnName) throws DDFException {
        IColumn column = this.getColumn(columnName);
        column.unsetAsFactor();
    }

    @Override
    public void unsetAsFactor(int columnIndex) throws DDFException {
        String columnName = this.getColumnName(columnIndex);
        this.unsetAsFactor(columnName);
    }

    @Override
    public void setFactorLevels(String columnName, IFactor factor) throws DDFException {
        IColumn column = this.getColumn(columnName);
        IFactor f = column.getFactor();
        if (f.getLevelCounts() != null) {
            f.setLevelCounts(factor.getLevelCounts());
        }
        if (f.getLevels() != null) {
            f.setLevels(factor.getLevels());
        }
    }

    @Override
    public void copyFactor(IDDF ddf) throws DDFException {
        this.copyFactor(ddf, null);
    }

    @Override
    public void copyFactor(IDDF ddf, List<String> columns) throws DDFException {
        if (columns != null) {
            for (IColumn column : ddf.getSchema().getColumns()) {
                if (this.associatedDDF.getSchema().getColumn(column.getName()) != null
                    && column.getFactor() != null) {
                    this.associatedDDF.getSchema().setAsFactor(column.getName());
                    if (!columns.contains(column.getName())) {
                        this.associatedDDF.getSchema().setFactorLevels(column.getName(), column.getFactor());
                    }
                }
            }
        }
        this.associatedDDF.getSchema().computeFactorLevelsAndLevelCounts();
    }

    @Override
    public void generateDummyCoding() throws NumberFormatException, DDFException {

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
            try {
                sb.append(column.getName() + ":" + column.getType().getSimpleName() + "|");
            }catch(Exception ex){
            }

        }
        if(sb.length()>0)
            sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }

    public static SchemaBuilder builder(){
        return new SchemaBuilder() {
            @Override
            public Schema newSchema() {
                return new Schema();
            }

            @Override
            protected Class inferType(String typeName) throws SchemaException {
                typeName = typeName.toLowerCase();
                switch (typeName){
                    case "byte":
                    case "smallint":
                    case "int":
                    case "integer":
                        return Integer.class;
                    case "float":
                    case "double":
                    case "number":
                        return Double.class;
                    case "bigdecimal":
                        return BigDecimal.class;
                    case "long":
                    case "bigint":
                        return Long.class;
                    case "string":
                        return String.class;
                    case "bool":
                    case "boolean":
                        return Boolean.class;
                    case "timestamp":
                        return Timestamp.class;
                    case "date":
                    case "datetime":
                        return java.sql.Date.class;
                    default:
                        throw new SchemaException("Couldn't inferType for " + typeName);
                }
            }
        };
    }

    public static abstract class SchemaBuilder<T extends Schema> {
        protected T schema;
        protected abstract T newSchema();

        protected abstract Class inferType(String typeName) throws SchemaException;
        public SchemaBuilder() {
            schema = newSchema();
        }

        /**
         * @param nameAndType A column name with type.
         *                    example. SchemaBuilder.add("username string").add("age int")
         * @return
         */
        protected SchemaBuilder _add(String nameAndType) throws SchemaException {
            String[] nameAndTypes = nameAndType.split(" ");
            if (nameAndTypes != null && nameAndTypes.length == 2) {
                add(nameAndTypes[0], inferType(nameAndTypes[1]));
            } else {
                throw new IllegalArgumentException("Wrong Format Expect: ColumnName Type");
            }
            return this;
        }

        /**
         * @param multiNameAndType a list of multi column name with type, seperate by comma
         *                    example. SchemaBuilder.add("username string, age int, birthdate date")
         * @return
         */
        public SchemaBuilder add(String multiNameAndType) throws SchemaException {
            String[] nameAndTypes = multiNameAndType.split(",");
            for(String nameAndType : nameAndTypes){
                _add(nameAndType);
            }
            return this;
        }

        public SchemaBuilder add(String colName, Class colType) {
            schema.append(new Column(colName, colType));
            return this;
        }
        public T build(){
            return schema;
        }
        /**
         * Fastest way to build
         * @param multiNameAndType a list of multi column name with type, seperate by comma
         *                    example. SchemaBuilder.add("username string, age int, birthdate date")
         * @return Schema
         */
        public T build(String multiNameAndType) throws SchemaException {
            this.add(multiNameAndType);
            return build();
        }
    }
}
