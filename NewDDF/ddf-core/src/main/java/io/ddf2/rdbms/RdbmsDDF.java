package io.ddf2.rdbms;

import io.ddf2.DDF;
import io.ddf2.IDDF;
import io.ddf2.IDDFResultSet;
import io.ddf2.datasource.IDataSource;

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
    public IDDFResultSet sql(String sql) {
        return null;
    }

    @Override
    protected long _getNumRows() {
        return 0;
    }
}
 
