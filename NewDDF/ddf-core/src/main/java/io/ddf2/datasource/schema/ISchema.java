package io.ddf2.datasource.schema;

import io.ddf2.DDFException;

import java.util.List;
import java.util.Set;

public interface ISchema {

	public int getNumColumn() ;
	 
	public List<IColumn> getColumns();

	public IColumn getColumn(String columnName) throws DDFException;

	public List<String> getColumnNames();

	public String getColumnName(int index) throws DDFException;

	public int getColumnIndex(String columnName) throws DDFException;
	 
}
 
