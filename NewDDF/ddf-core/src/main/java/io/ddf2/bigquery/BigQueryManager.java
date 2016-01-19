package io.ddf2.bigquery;

import io.ddf2.DDFException;
import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.IDataSource;

import java.util.Map;

/**
 * Created by sangdn on 1/18/16.
 */
public final class BigQueryManager extends DDFManager {

    protected BigQueryManager(Map mapProperties) throws DDFException {
        super(mapProperties);

    }

    @Override
    public IDDF newDDF(String name, IDataSource ds) throws DDFException {
        return null;
    }

    @Override
    public IDDF newDDF(IDataSource ds) throws DDFException {
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
