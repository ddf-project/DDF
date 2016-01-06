package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.handlers.IPersistentHandler;


/**
 * Each DDFManager have an unique UUID
 * Abstract factory, entry point of application to get concrete DDFManager (ex: SparkDDFManager)
 * Provide method to new IDDF & get IDDFMetaData
 * Manage all created DDF/DDFManager instance.
 * Provide method to get PersistentHandler to persist/remove/restore an DDF to storage.
 * 
 */
public interface IDDFManager {

	public IDDF newDDF(String name,IDataSource ds) throws DDFException;
	public IDDF newDDF(IDataSource ds) throws DDFException;
	public IDDFMetaData getDDFMetaData();
	public IPersistentHandler getPersistentHandler();
	public String getDDFManagerId();
	public ISqlResult sql(String query);
}
 
