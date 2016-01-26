package io.ddf2.bigquery;

import com.google.api.services.bigquery.Bigquery;
import io.ddf2.*;
import io.ddf2.bigquery.preparer.BigQueryPreparer;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 1/18/16.
 */
public class BigQueryDDF extends DDF {

    protected String projectId;
    protected String query;
    protected Bigquery bigquery;
    protected BigQueryDDF(IDataSource dataSource) {
        super(dataSource);
        bigquery = BigQueryUtils.newInstance();
    }



    /***
     * Init @mapDataSourcePreparer.
     * Add all supported DataSource to @mapDataSourcePreparer
     */
    @Override
    protected void initDSPreparer() {
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(BQDataSource.class,new BigQueryPreparer(bigquery));
    }

    /**
     * An reserve-function for concrete DDF to hook to build progress.
     */
    @Override
    protected void endBuild() {
        if(this.dataSource instanceof BQDataSource){
            this.numRows = ((BQDataSource)dataSource).getNumRows();
        }
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
    public IDDF sql2ddf(String sql) throws DDFException {
        BQDataSource bqDataSource = BQDataSource.builder().setProjectId(projectId).setQuery(sql).build();
        return ddfManager.newDDF(bqDataSource);
    }

    @Override
    protected long _getNumRows() {
        return numRows;
    }



    protected abstract static class BigQueryDDFBuilder<T extends BigQueryDDF> extends DDFBuilder<T> {
        public BigQueryDDFBuilder(IDataSource dataSource) {
            super(dataSource);
        }
    }
    public static BigQueryDDFBuilder<?> builder(IDataSource dataSource){
        return new BigQueryDDFBuilder<BigQueryDDF>(dataSource) {
            @Override
            public BigQueryDDF newInstance(IDataSource ds) {
                return new BigQueryDDF(ds);
            }

            @Override
            protected BigQueryDDF newInstance(String query) {
                return newInstance(BQDataSource.builder()
                                                .setProjectId((String) BigQueryContext.getProperty(BigQueryContext.KEY_PROJECT_ID))
                                                .setQuery(query)
                                                .build());
            }

            @Override
            public BigQueryDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
