package io.ddf2.datasource;

import io.ddf2.datasource.schema.ISchema;

/**
 * Created by sangdn on 12/30/15.
 */
public abstract class DataSource implements IDataSource {
    protected ISchema ISchema;
    protected  long createdTime;

    /**
     * @see io.ddf2.datasource.IDataSource#getISchema()
     */
    public ISchema getISchema() {
        return ISchema;
    }


    /**
     * @see io.ddf2.datasource.IDataSource#getNumColumn()
     */
    public abstract int getNumColumn();

    /**
     * @see io.ddf2.datasource.IDataSource#getCreatedTime()
     */
    public long getCreatedTime() {
        return createdTime;
    }

}
