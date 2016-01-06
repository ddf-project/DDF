package io.ddf2.rdbms;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;

import java.util.Map;

public class RdbmsDDFManager extends DDFManager {

    protected RdbmsDDFManager(Map mapProperties) {
        super(mapProperties);
    }

    @Override
    public IDDF newDDF(String name, IDataSource ds) throws DDFException {
        return null;
    }

    /**
     * @param ds
     * @see IDDFManager#newDDF(IDataSource)
     */
    @Override
    public IDDF newDDF(IDataSource ds) {
        return null;
    }

    @Override
    public String getDDFManagerId() {
        return null;
    }

    @Override
    public ISqlResult sql(String query) {
        return null;
    }
}
 
