package io.ddf2.rdbms;

import io.ddf2.DDF;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;

import java.util.Map;

public class RdbmsDDF extends DDF {

    protected RdbmsDDF(IDataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected void build(Map mapDDFProperties) {

    }

    /**
     * @param sql
     * @see IDDF#sql(String)
     */
    @Override
    public ISqlResult sql(String sql) {
        return null;
    }

    @Override
    protected long _getNumRows() {
        return 0;
    }

    @Override
    protected IDataSourcePreparer getDataSourcePreparer() {
        return null;
    }
}
 
