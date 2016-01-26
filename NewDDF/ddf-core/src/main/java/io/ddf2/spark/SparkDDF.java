package io.ddf2.spark;

import io.ddf2.*;
import io.ddf2.DDF;
import io.ddf2.DDFException;
import io.ddf2.IDDF;
import io.ddf2.ISqlResult;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.spark.preparer.LocalFilePreparer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;


import java.lang.Override;
import java.lang.String;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * SparkDDF implement DDF using HiveContext
 * @see io.ddf2.IDDF
 */
public class SparkDDF extends DDF {

    protected Map<Class,IDataSourcePreparer> mapDataSourcePreparer;
    protected SparkContext sparkContext;
    protected HiveContext hiveContext;

    public static final String PROPERTY_HIVE_CONTEXT = "HIVE_CONTEXT";
    public static final String PROPERTY_SPARK_CONTEXT = "SPARK_CONTEXT";

    SparkDDF(IDataSource dataSource) {
        super(dataSource);

    }
    @Override
    protected void beforeBuild(Map mapDDFProperties) {
        sparkContext = (SparkContext)mapDDFProperties.get(PROPERTY_SPARK_CONTEXT);
        if(sparkContext == null) throw new RuntimeException("SparkDDF required to have SparkContext On DdfProperties");

        hiveContext = (HiveContext)mapDDFProperties.get(PROPERTY_HIVE_CONTEXT);
        if(hiveContext == null) throw new RuntimeException("SparkDDF required to have HiveContext On DdfProperties");

    }
    @Override
    protected void initDSPreparer() {
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(LocalFileDataSource.class,new LocalFilePreparer(hiveContext));
    }


    /**
     * @param sql
     * @see IDDF#sql(String)
     */
    @Override
    public ISqlResult sql(String sql) {
        DataFrame dataFrame = hiveContext.sql(sql);

       return SparkUtils.dataFrameToSqlResult(dataFrame);
    }

    @Override
    public IDDF sql2ddf(String sql) throws DDFException {
        return ddfManager.newDDF(SqlDataSource.builder().setQuery(sql).build());
    }

    @Override
    public IDDF sql2ddf(String sql, Map<String, String> options) throws DDFException {
        return null;
    }

    @Override
    public ISqlResult sql(String sql, Map<String, String> options) throws SQLException {
        return null;
    }

    @Override
    protected long _getNumRows() {
        return 0;
    }




    protected abstract static class SparkDDFBuilder<T extends SparkDDF> extends DDFBuilder<T> {
        public SparkDDFBuilder(IDataSource dataSource) {
            super(dataSource);
        }
    }
    public static SparkDDFBuilder<?> builder(IDataSource dataSource){
        return new SparkDDFBuilder<SparkDDF>(dataSource) {
            @Override
            public SparkDDF newInstance(IDataSource ds) {
                return new SparkDDF(ds);
            }

            @Override
            protected SparkDDF newInstance(String query) {
                SqlDataSource ds = SqlDataSource.builder().setQuery(query).build();
                return newInstance(ds);
            }

            @Override
            public SparkDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
 
