package io.ddf2;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import io.ddf2.datasource.schema.ISchema;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Provide all utility function (getAllTable, dropDDF, getDDFSchema …)
 * 
 */
public interface IDDFMetaData {
 
	public Set<String> getAllDDFNames() throws DDFException;
	public  Set<Pair<String,ISchema>> getAllDDFNameWithSchema() throws DDFException;
	public ISchema getDDFSchema(String ddfName) throws DDFException;
	public  int dropAllDDF();
	public  int getNumDDF() throws DDFException;
	public  boolean dropDDF(String ddfName) throws IOException;
	public long getCreationTime(String ddfName) throws DDFException;


}
 
