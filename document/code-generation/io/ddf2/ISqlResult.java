package io.ddf2;

import io.ddf2.datasource.ISchema;
import java.util.Date;

public interface ISqlResult {
 
	public void close();
	public ISchema getSchema();
	public void first();
	public String getRaw();
	public Object get(int index);
	public Object get(String name);
	public Object get(int index, Object default);
	public Object get(String name, Object default);
	public int getInt(int index);
	public int getInt(String name);
	public int getInt(int index, int default);
	public int getInt(String name, int default);
	public long getLong(int index);
	public long getLong(String name);
	public long getLong(int index, long default);
	public long getLong(String name, long default);
	public double getDouble(int index);
	public double getDouble(String name);
	public long getDouble(int index, long default);
	public double getDouble(String name, double default);
	public Date getDate(int index);
	public Date getDate(String name);
	public Date getDate(int index, Date default);
	public Date getDate(String name, Date default);
	public String getString(int index);
	public String getString(String name);
	public String getString(int index, String default);
	public String getString(String name, String default);
	public IDDFType getType(int index);
	public IDDFType getType(String name);
	public IDDFType getType(int index, IDDFType default);
	public IDDFType getType(String name, IDDFType default);
}
 
