package io.ddf2.rdbms;

import io.ddf2.DDF;
import io.ddf2.IDDF;
import io.ddf2.IDDFResultSet;

public class RdbmsDDF extends DDF {

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
 
