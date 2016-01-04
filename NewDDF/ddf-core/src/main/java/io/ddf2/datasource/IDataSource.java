package io.ddf2.datasource;

import io.ddf2.datasource.schema.ISchema;


/**
 * Datasource contains all info of data 
 * Datasource take responsible to answer these questions: 
 * where does data store, what does data store, how do we retrieve it, what is it ISchema
 * 
 */
public interface IDataSource {
	public ISchema getISchema();
	public int getNumColumn();
	public long getCreatedTime();
}
 
