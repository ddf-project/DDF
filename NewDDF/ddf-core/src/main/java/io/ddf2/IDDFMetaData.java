package io.ddf2;

import java.util.List;
import java.util.Set;

import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Provide all utility function (getAllTable, dropDDF, getDDFSchema …)
 * 
 */
public interface IDDFMetaData {
 
	public Set<String> getAllDDFNames();
	public  Set<Pair<String,ISchema>> getAllDDFNameWithSchema();
	public ISchema getDDFSchema(String ddfName);
	public  int dropAllDDF();
	public  int getNumDDF();
	public  boolean dropDDF(String ddfName);
	public long getCreationTime(String ddfName);


}
 
