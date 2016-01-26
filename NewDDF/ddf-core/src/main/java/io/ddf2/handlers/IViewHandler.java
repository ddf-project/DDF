package io.ddf2.handlers;

import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.ISqlResult;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public interface IViewHandler extends IDDFHandler {
    /**
     * @param numSamples
     * @return a new DDF containing `numSamples` rows selected randomly from our owner DDF.
     */
    public ISqlResult getRandomSample(int numSamples, boolean withReplacement, int seed);

    public DDF getRandomSampleByNum(int numSamples, boolean withReplacement, int seed);

    public DDF getRandomSample(double percent, boolean withReplacement, int seed);

    public ISqlResult head(int numRows) throws DDFException;

    public ISqlResult top(int numRows, String orderCols, String mode) throws DDFException;

    public DDF project(String... columnNames) throws DDFException;

    public DDF project(List<String> columnNames) throws DDFException;

    public DDF subset(List<Column> columnExpr, Expression filter) throws DDFException;

    public DDF removeColumn(String columnName) throws DDFException;

    public DDF removeColumns(String... columnNames) throws DDFException;

    public DDF removeColumns(List<String> columnNames) throws DDFException;




    /**
     * Base class for any Expression node in the AST, could be either an Operator or a Value
     */
    static public class Expression implements Serializable {
        String type;


        public String getType() {
            return type;
        }

        public String toSql() {
            return null;
        }

        public void setType(String aType) {
            type = aType;
        }
    }


    public enum OperationName {
        lt, le, eq, ge, gt, ne, and, or, neg, isnull, isnotnull, grep, grep_ic
    }


    /**
     * Base class for unary operations and binary operations
     */
    static public class Operator extends Expression {
        OperationName name;
        Expression[] operands;

        public Operator() {
            super.setType("Operator");
        }

        public OperationName getName() {
            return name;
        }


        public Expression[] getOperands() {
            return operands;
        }

        public void setOperarands(Expression[] ops) {
            this.operands = ops;
        }

        public void setName(OperationName name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Operator [name=" + name + ", operands=" + Arrays.toString(operands) + "]";
        }

        @Override
        public String toSql() {
            if (name == null) {
                throw new IllegalArgumentException("Missing Operator name from Adatao client for operands[] " + Arrays.toString(operands));
            }
            switch (name) {
                case gt:
                    return String.format("(%s > %s)", operands[0].toSql(), operands[1].toSql());
                case lt:
                    return String.format("(%s < %s)", operands[0].toSql(), operands[1].toSql());
                case ge:
                    return String.format("(%s >= %s)", operands[0].toSql(), operands[1].toSql());
                case le:
                    return String.format("(%s <= %s)", operands[0].toSql(), operands[1].toSql());
                case eq:
                    return String.format("(%s = %s)", operands[0].toSql(), operands[1].toSql());
                case ne:
                    return String.format("(%s != %s)", operands[0].toSql(), operands[1].toSql());
                case and:
                    return String.format("(%s AND %s)", operands[0].toSql(), operands[1].toSql());
                case or:
                    return String.format("(%s OR %s)", operands[0].toSql(), operands[1].toSql());
                case neg:
                    return String.format("(NOT %s)", operands[0].toSql());
                case isnull:
                    return String.format("(%s IS NULL)", operands[0].toSql());
                case isnotnull:
                    return String.format("(%s IS NOT NULL)", operands[0].toSql());
                case grep:
                    return String.format("(%s LIKE '%%%s%%')", operands[1].toSql(), operands[0].toSql().substring(1, operands[0].toSql().length() - 1));
                case grep_ic:
                    return String.format("(lower(%s) LIKE '%%%s%%')", operands[1].toSql(), operands[0].toSql().substring(1, operands[0].toSql().length() - 1).toLowerCase());
                default:
                    throw new IllegalArgumentException("Unsupported Operator: " + name);
            }
        }
    }


    public abstract static class Value extends Expression {
        public abstract Object getValue();

        public void setType(String type) {
            super.setType(type);
        }
    }


    static public class IntVal extends Value {
        int value;

        public IntVal() {
            super.setType("IntVal");
        }


        @Override
        public String toString() {
            return "IntVal [value=" + value + "]";
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String toSql() {
            return Integer.toString(value);
        }
    }


    static public class DoubleVal extends Value {
        double value;

        public DoubleVal() {
            super.setType("DoubleVal");
        }


        @Override
        public String toString() {
            return "DoubleVal [value=" + value + "]";
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String toSql() {
            return Double.toString(value);
        }
    }


    static public class StringVal extends Value {
        String value;

        public StringVal() {
            super.setType("StringVal");
        }

        public void setValue(String val) {
            this.value = val;
        }

        @Override
        public String toString() {
            return "StringVal [value=" + value + "]";
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String toSql() {
            String s = value.replaceAll("'", "\\\\'");
            return String.format("'%s'", s);
        }
    }


    static public class BooleanVal extends Value {
        Boolean value;

        public BooleanVal() {
            super.setType("BooleanVal");
        }


        @Override
        public String toString() {
            return "BooleanVal [value=" + value + "]";
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String toSql() {
            return Boolean.toString(value);
        }
    }


    static public class Column extends Expression {
        String id;
        String name;
        Integer index = null;

        public Column() {
            super.setType("Column");
        }


        public String getID() {
            return id;
        }

        public Integer getIndex() {
            return index;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }

        public void setID(String id) {
            this.id = id;
        }

        public Object getValue(Object[] xs) {
            return xs[index];
        }

        @Override
        public String toSql() {
            assert this.name != null;
            return this.name;
        }

        @Override
        public String toString() {
            return "Column [id=" + id + ", name=" + name + ", index=" + index + "]";
        }

        public String getName() {
            return name;
        }
    }

}

 
