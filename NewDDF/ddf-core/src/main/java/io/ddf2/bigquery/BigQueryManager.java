package io.ddf2.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.handlers.IPersistentHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sangdn on 1/18/16.
 * @see io.ddf2.IDDFManager
 */
public final class BigQueryManager extends DDFManager<BigQueryDDF> {


    protected BigQueryManager(Map mapProperties) throws DDFException {
        super(mapProperties);

    }

    @Override
    public BigQueryDDF _newDDF(String name, IDataSource ds) throws DDFException {
        return  (BigQueryDDF) BigQueryDDF.builder(ds)
                .setName(name)
                .putProperty(mapProperties)
                .setDDFManager(this)
                .build();
    }

    @Override
    protected IPersistentHandler _getPersistentHanlder() {
        return null;
    }

    @Override
    protected IDDFMetaData _getDDFMetaData() {
        return new BigQueryMetaData();
    }


    @Override
    public ISqlResult sql(String query) throws SQLException {
        try {
            Bigquery bigquery = BigQueryUtils.newInstance();
            String projectId = BigQueryContext.getProjectId();
            QueryResponse response = bigquery.jobs().query(projectId, new QueryRequest().setQuery(query)).execute();
            return new BigQuerySqlResult(response);
        } catch (IOException e) {
            throw new SQLException("couldn't execute sql exception msg:" + e.getMessage());
        }
    }

    @Override
    public ISqlResult sql(String query, Map options) throws SQLException {
        return sql(query);
    }


}
