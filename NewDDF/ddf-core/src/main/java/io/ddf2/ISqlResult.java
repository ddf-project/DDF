package io.ddf2;

import io.ddf2.datasource.schema.ISchema;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

public interface ISqlResult extends AutoCloseable {
 
	public ISchema getSchema();
	public void first();
	public boolean next();
	public Object getRaw();
	public Object get(int index);
	public Object get(String name);
	public Object get(int index, Object defaultValue);
	public Object get(String name, Object defaultValue);
	public int getInt(int index);
	public int getInt(String name);
	public int getInt(int index, int defaultValue);
	public int getInt(String name, int defaultValue);
	public Boolean getBoolean(int index);
	public Boolean getBoolean(String name);
	public Boolean getBoolean(int index, Boolean defaultValue);
	public Boolean getBoolean(String name, Boolean defaultValue);
	public long getLong(int index);
	public long getLong(String name);
	public long getLong(int index, long defaultValue);
	public long getLong(String name, long defaultValue);
	public double getDouble(int index);
	public double getDouble(String name);
	public double getDouble(int index, double defaultValue);
	public double getDouble(String name, double defaultValue);
	public Date getDate(int index) throws ParseException;
	public Date getDate(String name);
	public Date getDate(int index, Date defaultValue);
	public Date getDate(String name, Date defaultValue);
	public String getString(int index);
	public String getString(String name);
	public String getString(int index, String defaultValue);
	public String getString(String name, String defaultValue);
	public<T extends IDDFType> T getType(int index,Class<T> cls) throws SQLException;
	public <T extends IDDFType> T  getType(String name,Class<T> cls);
	public <T extends IDDFType> T  getType(int index,Class<T> cls, T defaultValue);
	public <T extends IDDFType> T  getType(String name,Class<T> cls,T defaultValue);
}
 
