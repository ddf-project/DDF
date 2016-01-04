package io.ddf2.rdbms;

import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.datasource.IDataSource;

import java.util.Map;

public class RdbmsDDFManager extends DDFManager {

    protected RdbmsDDFManager(Map mapProperties) {
        super(mapProperties);
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
}
 
