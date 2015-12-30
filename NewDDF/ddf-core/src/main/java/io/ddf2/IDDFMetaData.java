package io.ddf2;

import java.util.List;
import io.ddf2.datasource.schema.Schema;

/**
 * Provide all utility function (getAllTable, dropTable, getTableSchema …)
 * 
 */
public interface IDDFMetaData {
 
	public  List<String> getAllTables();
	public  List getAllTablesWithSchema();
	public  Schema getTableSchema(String tblName);
	public  int dropAllTables();
	public  int getNumTables();
	public  boolean dropTable(String tblName);
}
 
