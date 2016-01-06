package io.ddf2.rdbms;

import io.ddf2.IDDFMetaData;
import java.util.List;

import io.ddf2.datasource.schema.ISchema;

public class RdbmsDDFMetadata implements IDDFMetaData {
 
	/**
	 * @see io.ddf2.IDDFMetaData#getAllDDFNames()
	 */
	public List<String> getAllDDFNames() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getAllDDFNameWithSchema()
	 */
	public List getAllDDFNameWithSchema() {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getDDFSchema(java.lang.String)
	 */
	public ISchema getDDFSchema(String ddfName) {
		return null;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropAllDDF()
	 */
	public int dropAllDDF() {
		return 0;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#getNumDDF()
	 */
	public int getNumDDF() {
		return 0;
	}
	 
	/**
	 * @see io.ddf2.IDDFMetaData#dropDDF(java.lang.String)
	 */
	public boolean dropDDF(String ddfName) {
		return false;
	}
	 
}
 
