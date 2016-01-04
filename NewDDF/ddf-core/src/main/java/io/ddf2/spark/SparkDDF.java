package io.ddf2.spark;

import io.ddf2.DDF;
import io.ddf2.IDDF;
import io.ddf2.IDDFResultSet;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.SqlDataSource;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;


import java.util.Map;

public class SparkDDF extends DDF {

    protected JavaSparkContext javaSparkContext;
    protected SQLContext sqlContext;

    SparkDDF(IDataSource dataSource) {
        super(dataSource);
    }

    /**
     * @see DDF#build(Map)
     * @param mapDDFProperties required to contains JavaSparkContext
     */
    @Override
    protected void build(Map mapDDFProperties) {
        JavaSparkContext javaSparkContext = (JavaSparkContext)mapDDFProperties.get("JavaSparkContext");
        if(javaSparkContext == null) throw new RuntimeException("SparkDDF required to have JavaSparkContext On DdfProperties");
        this.javaSparkContext = javaSparkContext;
        this.sqlContext = new SQLContext(javaSparkContext);
        this.mapDDFProperties = mapDDFProperties;
        resolveDataSource();
    }

    /**
     *
     */
    private void resolveDataSource() {

    }


    /**
     * @param sql
     * @see IDDF#sql(String)
     */
    @Override
    public IDDFResultSet sql(String sql) {
        IDDFResultSet resultSet;
        return null;

    }

    @Override
    protected long _getNumRows() {
        return 0;
    }

    @Override
    protected IDataSourcePreparer getDataSourcePreparer() {
        return null;
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

            @Override
            public SparkDDF build() {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
 
