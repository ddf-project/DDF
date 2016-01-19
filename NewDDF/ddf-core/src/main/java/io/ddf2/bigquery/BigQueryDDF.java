package io.ddf2.bigquery;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

/**
 * Created by sangdn on 1/18/16.
 */
public class BigQueryDDF extends DDF {

    protected BigQueryDDF(IDataSource dataSource) {
        super(dataSource);
    }

    /**
     * Finally build DDF. Called from builder.
     *
     * @param mapDDFProperties is a contract between concrete DDFManager & concrete DDF
     */
    @Override
    protected void build(Map mapDDFProperties) throws PrepareDataSourceException, UnsupportedDataSourceException {

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
