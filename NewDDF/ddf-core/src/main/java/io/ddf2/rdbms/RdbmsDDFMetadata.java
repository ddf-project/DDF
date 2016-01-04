package io.ddf2.rdbms;

import io.ddf2.IDDFMetaData;
import java.util.List;

import io.ddf2.datasource.schema.ISchema;

public class RdbmsDDFMetadata implements IDDFMetaData {
 
	/**
	 * @see io.ddf2.IDDFMetaData#getAllTables()
	 */
	public List<String> getAllTables() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getAllTablesWithSchema()
	 */
	public List getAllTablesWithSchema() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getTableSchema(java.lang.String)
	 */
	public ISchema getTableSchema(String tblName) {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropAllTables()
	 */
	public int dropAllTables() {
		return 0;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getNumTables()
	 */
	public int getNumTables() {
		return 0;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropTable(java.lang.String)
	 */
	public boolean dropTable(String tblName) {
		return false;
	}
	 
}
 
