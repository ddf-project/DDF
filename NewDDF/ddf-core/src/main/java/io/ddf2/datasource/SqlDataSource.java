package io.ddf2.datasource;

/**
 * Created by sangdn on 12/30/15.
 */
public final class SqlDataSource extends DataSource{


    protected String sqlQuery;

    protected SqlDataSource(){
    }

    public String getSqlQuery() {
        return sqlQuery;
    }


    public static Builder<SqlDataSource> builder(){
        return new Builder<SqlDataSource>() {
            @Override
            protected SqlDataSource newInstance() {
                return new SqlDataSource();
            }
        };
    }
    public abstract static class Builder<T extends SqlDataSource> extends DataSource.Builder<T>{

        public Builder<T> setQuery(String sqlQuery){
            datasource.sqlQuery = sqlQuery;
            return this;
        }
    }



}
