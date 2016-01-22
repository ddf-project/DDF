package io.ddf2.bigquery;

import io.ddf2.*;
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
    protected BigQueryDDF(IDataSource dataSource) {
        super(dataSource);
    }

    /***
     * DDFManager will pass ddfProperties to concreted DDF thanks to our contraction.
     *
     * @param mapDDFProperties
     */
    @Override
    protected void _initWithProperties(Map mapDDFProperties) {
        // Not using any property from BigQueryDDFManager
    }

    /***
     * Init @mapDataSourcePreparer.
     * Add all supported DataSource to @mapDataSourcePreparer
     */
    @Override
    protected void _initDSPreparer() {
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(BQDataSource.class,BQDataSourcePreparer)
    }


    protected void resolveDataSource() throws PrepareDataSourceException{

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
        BQDataSource bqDataSource = new BQDataSource(projectId,sql);
        return ddfManager.newDDF(bqDataSource);
    }

    @Override
    protected long _getNumRows() {
        return 0;
    }

    /**
     * @return
     * @see IDataSourcePreparer
     */
    @Override
    protected IDataSourcePreparer getDataSourcePreparer() throws UnsupportedDataSourceException {
        return null;
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
            protected BigQueryDDF newInstance(String ds) {
                //Todo: ThreadLocal to implement this feature
                throw new NotImplementedException("Not Implement Yet");
            }

            @Override
            public BigQueryDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
