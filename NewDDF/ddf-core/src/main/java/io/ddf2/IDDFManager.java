package io.ddf2;

import io.ddf2.datasource.IDataSource;
import io.ddf2.handlers.IPersistentHandler;


/**
 * Each DDFManager have an unique UUID, will be Singleton by Concrete & mapProperties
 * Abstract factory, entry point of application to get concrete DDFManager (ex: SparkDDFManager)
 * Provide method to new IDDF & get IDDFMetaData
 * Manage all created DDF/DDFManager instance.
 * Provide method to get PersistentHandler to persist/remove/restore an DDF to storage.
 * 
 */
public interface IDDFManager {

	public IDDF newDDF(IDataSource ds);
	public IDDF newDDF(String sql);
	public IDDFMetaData getDDFMetaData();
	public IPersistentHandler getPersistentHandler();
}
 
