package io.ddf2.datasource.schema;

import io.ddf2.DDFException;
import io.ddf2.DDF;

import java.util.List;
import java.util.Set;

public interface ISchema {

	public int getNumColumn() ;
	 
	public List<IColumn> getColumns();

	public IColumn getColumn(String columnName) throws DDFException;

	public List<String> getColumnNames();

	public String getColumnName(int index) throws DDFException;

	public int getColumnIndex(String columnName) throws DDFException;

	// TODO: @sang, rethink here. after we put the factor related apis in schema, the schema becomes engine specific (
	// because implementation of factor is engine specific). It's better to have separate handlers to handle this?
	void computeFactorLevelsAndLevelCounts() throws DDFException;

	void setFactorLevelsForStringColumns(String[] xCols) throws DDFException;

	IFactor setAsFactor(String columnName) throws DDFException;

	IFactor setAsFactor(int columnIndex) throws DDFException;

	void unsetAsFactor(String columnName) throws DDFException;

	void unsetAsFactor(int columnIndex) throws DDFException;

	void setFactorLevels(String columnName, IFactor factor) throws DDFException;

	void generateDummyCoding() throws NumberFormatException, DDFException;

	void copyFactor(DDF ddf) throws DDFException;

	void copyFactor(DDF ddf, List<String> columns) throws DDFException;
}
 
