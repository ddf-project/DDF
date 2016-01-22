package io.ddf2.bigquery.preparer;

import com.google.api.services.bigquery.Bigquery;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;

/**
 * Created by sangdn on 1/22/16.
 * BigQueryPreparer will prepare for BigQueryDDF
 * 1/ Create VIEW from BigQueryDataSource
 * 2/ Execute with Limit 1 to get Schema
 *
 */
public class BigQueryPreparer implements IDataSourcePreparer {
    protected Bigquery bigquery;
    public BigQueryPreparer(Bigquery bigQuery){
        this.bigquery = bigquery;
    }
    @Override
    public IDataSource prepare(String ddfName, IDataSource dataSource) throws PrepareDataSourceException {
        bigquery.jobs().Insert.
    }
}
