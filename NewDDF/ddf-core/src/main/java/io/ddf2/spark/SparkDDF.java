package io.ddf2.spark;

import io.ddf2.DDF;
import io.ddf2.IDDF;
import io.ddf2.IDDFResultSet;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.SqlDataSource;

public class SparkDDF extends DDF {

    SparkDDF(IDataSource dataSource) {
        super(dataSource);
    }

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

    protected abstract static class Builder<T extends SparkDDF> extends DDF.Builder<T> {
        public Builder(IDataSource dataSource) {
            super(dataSource);
        }
    }

    public static Builder<?> builder(IDataSource dataSource){
        return new Builder<SparkDDF>(dataSource) {
            @Override
            public SparkDDF newInstance(IDataSource ds) {
                return new SparkDDF(ds);
            }

            @Override
            protected SparkDDF newInstance(String ds) {
                return newInstance(new SqlDataSource(ds));
            }
        };
    }
}
 
