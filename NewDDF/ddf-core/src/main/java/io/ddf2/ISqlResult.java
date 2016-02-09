package io.ddf2;

import io.ddf2.datasource.schema.ISchema;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

/**
 * ISqlResult is root-interface for any sqlresult return from DDF
 */
public interface ISqlResult extends AutoCloseable {
 
	public ISchema getSchema();
	public void first();
	public boolean next();
	public Object getRaw();
	public Object get(int index);
	public Object get(String name);
	public Object get(int index, Object defaultValue);
	public Object get(String name, Object defaultValue);
	public Integer getInt(int index);
	public Integer getInt(String name);
	public Integer getInt(int index, int defaultValue);
	public Integer getInt(String name, int defaultValue);
	public Boolean getBoolean(int index);
	public Boolean getBoolean(String name);
	public Boolean getBoolean(int index, Boolean defaultValue);
	public Boolean getBoolean(String name, Boolean defaultValue);
	public Long getLong(int index);
	public Long getLong(String name);
	public Long getLong(int index, long defaultValue);
	public Long getLong(String name, long defaultValue);
	public Double getDouble(int index);
	public Double getDouble(String name);
	public Double getDouble(int index, double defaultValue);
	public Double getDouble(String name, double defaultValue);
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
 
