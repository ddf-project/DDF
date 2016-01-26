package io.ddf2;

import java.util.List;
import io.ddf2.datasource.schema.ISchema;

/**
 * Provide all utility function (getAllTable, dropDDF, getDDFSchema …)
 * 
 */
public interface IDDFMetaData {
 
	public  List<String> getAllDDFNames();
	public  List getAllDDFNameWithSchema();
	public ISchema getDDFSchema(String ddfName);
	public  int dropAllDDF();
	public  int getNumDDF();
	public  boolean dropDDF(String ddfName);

}
 
