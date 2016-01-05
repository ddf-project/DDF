package io.ddf2.datasource;

import io.ddf2.datasource.schema.ISchema;

/**
 * Created by sangdn on 12/30/15.
 */
public abstract class DataSource implements IDataSource {
    protected ISchema schema;
    protected  long createdTime;

    /**
     * @see io.ddf2.datasource.IDataSource#getSchema()
     */
    public ISchema getSchema() {
        return schema;
    }


    /**
     * @see io.ddf2.datasource.IDataSource#getNumColumn()
     */
    public int getNumColumn(){
        if(schema != null){
            return schema.getNumColumn();
        }
        return -1;
    }

    /**
     * @see io.ddf2.datasource.IDataSource#getCreatedTime()
     */
    public long getCreatedTime() {
        return createdTime;
    }


    public abstract static class Builder<T extends DataSource>{
        protected T datasource;
        protected abstract T newInstance();


        public Builder(){
            datasource = newInstance();
        }
        public T build(){
            return datasource;
        }
        public Builder<T> setSchema(ISchema schema){
            datasource.schema = schema;
            return  this;
        }
        public Builder<T> setCreatedTime(long createdTime){
            datasource.createdTime = createdTime;
            return  this;
        }

    }
}
