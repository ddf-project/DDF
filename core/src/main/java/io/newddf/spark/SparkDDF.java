package io.newddf.spark;

import com.sun.istack.Nullable;
import io.newddf.DDF;
import io.newddf.datasource.IDataSource;

import java.util.Map;

/**
 * Created by sangdn on 12/28/15.
 */
public final class SparkDDF extends DDF {

    private SparkDDF(IDataSource dataSource) {
        super(dataSource);

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
        };
    }
}
