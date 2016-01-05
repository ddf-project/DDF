package io.ddf2;

import io.ddf2.datasource.schema.ISchema;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by sangdn on 1/5/16.
 */
public class SqlResult implements ISqlResult {

    protected ISchema schema;
    protected Iterable<String[]> itResult;
    protected Iterator<String[]> iterator;

    protected String[] columns;

    protected DateFormat dateFormat = new SimpleDateFormat("dd MM yyyy HH:mm:ss");

    public SqlResult(ISchema schema,Iterable<String[]> iterable){
        this.schema = schema;
        this.itResult = iterable;
        iterator = itResult.iterator();
    }
    @Override
    public void close() {
        itResult = null;
        iterator = null;
    }

    @Override
    public ISchema getSchema() {
        return schema;
    }

    @Override
    public void first() {
        iterator = itResult.iterator();
    }

    @Override
    public boolean next() {
        if(iterator.hasNext()){
            columns =iterator.next();

            return true;
        }
        return  false;
    }

    @Override
    public Object getRaw() {
        return String.join("\\t", columns);
    }

    @Override
    public Object get(int index) {
        return columns[index];
    }

    @Override
    public Object get(String name) {
        return null;
    }

    @Override
    public Object get(int index, Object defaultValue) {
        return null;
    }

    @Override
    public Object get(String name, Object defaultValue) {
        return null;
    }

    @Override
    public int getInt(int index) {
        return Integer.parseInt(columns[index]);
    }

    @Override
    public int getInt(String name) {
        return 0;
    }

    @Override
    public int getInt(int index, int defaultValue) {
        return 0;
    }

    @Override
    public int getInt(String name, int defaultValue) {
        return 0;
    }

    @Override
    public Boolean getBoolean(int index) {
        return null;
    }

    @Override
    public Boolean getBoolean(String name) {
        return null;
    }

    @Override
    public Boolean getBoolean(int index, Boolean defaultValue) {
        return null;
    }

    @Override
    public Boolean getBoolean(String name, Boolean defaultValue) {
        return null;
    }

    @Override
    public long getLong(int index) {
        return Long.parseLong(columns[index]);
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public long getLong(int index, long defaultValue) {
        return 0;
    }

    @Override
    public long getLong(String name, long defaultValue) {
        return 0;
    }

    @Override
    public double getDouble(int index) {
        return Double.parseDouble(columns[index]);
    }

    @Override
    public double getDouble(String name) {
        return 0;
    }

    @Override
    public long getDouble(int index, long defaultValue) {
        return 0;
    }

    @Override
    public double getDouble(String name, double defaultValue) {
        return 0;
    }

    @Override
    public Date getDate(int index) throws ParseException {
        return dateFormat.parse(columns[index]);
    }

    @Override
    public Date getDate(String name) {
        return null;
    }

    @Override
    public Date getDate(int index, Date defaultValue) {
        return null;
    }

    @Override
    public Date getDate(String name, Date defaultValue) {
        return null;
    }

    @Override
    public String getString(int index) {
        return columns[index];
    }

    @Override
    public String getString(String name) {
        return null;
    }

    @Override
    public String getString(int index, String defaultValue) {
        return null;
    }

    @Override
    public String getString(String name, String defaultValue) {
        return null;
    }

    @Override
    public<T extends IDDFType> T  getType(int index,Class<T> cls) throws SQLException {
        try {
            T instance = cls.newInstance();
            instance.parse(columns[index]);
            return instance;
        } catch (Exception e) {
            throw new SQLException("couldn't parse to " + cls.getSimpleName());
        }
    }

    @Override
    public <T extends IDDFType> T getType(String name, Class<T> cls) {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(int index, Class<T> cls, T defaultValue) {
        return null;
    }

    @Override
    public <T extends IDDFType> T getType(String name, Class<T> cls, T defaultValue) {
        return null;
    }


}
