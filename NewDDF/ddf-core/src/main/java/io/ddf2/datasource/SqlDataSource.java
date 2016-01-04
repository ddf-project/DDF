package io.ddf2.datasource;

/**
 * Created by sangdn on 12/30/15.
 */
public class SqlDataSource extends DataSource{

    protected String sqlQuery;

    public SqlDataSource(String sqlQuery){
        this.sqlQuery = sqlQuery;
    }

    /**
     * @see IDataSource#getNumColumn()
     */
    @Override
    public int getNumColumn() {
        return 0;
    }
}
