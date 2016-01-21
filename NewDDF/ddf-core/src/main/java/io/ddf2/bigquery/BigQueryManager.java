package io.ddf2.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import io.ddf2.*;
import io.ddf2.datasource.IDataSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sangdn on 1/18/16.
 */
public final class BigQueryManager extends DDFManager {
    protected BigQueryManager(Map mapProperties) throws DDFException {
        super(mapProperties);

    }

    @Override
    public IDDF _newDDF(String name, IDataSource ds) throws DDFException {
        return null;
    }



    @Override
    public ISqlResult sql(String query) throws SQLException {
        try {
            Bigquery bigquery = BigQueryUtils.newInstance();
            String projectId = (String) BigQueryContext.getProperty(BigQueryContext.KEY_PROJECT_ID);
            QueryResponse response = bigquery.jobs().query(projectId, new QueryRequest().setQuery(query)).execute();
            return new BigQuerySqlResult(response);
        } catch (IOException e) {
            throw new SQLException("couldn't execute sql exception msg:" + e.getMessage());
        }
    }
}
