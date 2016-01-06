package io.ddf2.spark;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.spark.preparer.SparkLocalFilePreparer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;


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

    /**
     * @see DDF#build(Map)
     * @param mapDDFProperties required to contains JavaSparkContext
     */
    @Override
    protected void build(Map mapDDFProperties) throws PrepareDataSourceException {

        sparkContext = (SparkContext)mapDDFProperties.get(PROPERTY_SPARK_CONTEXT);
        if(sparkContext == null) throw new RuntimeException("SparkDDF required to have SparkContext On DdfProperties");

        hiveContext = (HiveContext)mapDDFProperties.get(PROPERTY_HIVE_CONTEXT);
        if(hiveContext == null) throw new RuntimeException("SparkDDF required to have HiveContext On DdfProperties");


        this.mapDDFProperties = mapDDFProperties;
        //all support datasource preparer
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(LocalFileDataSource.class,new SparkLocalFilePreparer(sparkContext,hiveContext));
        resolveDataSource();
    }

    /**
     *
     */
    private void resolveDataSource() throws PrepareDataSourceException {
        IDataSourcePreparer preparer = null;
        try {
            preparer = getDataSourcePreparer();
        } catch (UnsupportedDataSourceException e) {
            e.printStackTrace();
            throw new PrepareDataSourceException("Not find DataSourcePreparer For " + dataSource.getClass().getSimpleName());
        }
        preparer.prepare(name, dataSource);
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
    protected long _getNumRows() {
        return 0;
    }

    @Override
    protected IDataSourcePreparer getDataSourcePreparer() throws UnsupportedDataSourceException {
        if(mapDataSourcePreparer.containsKey(dataSource.getClass())){
            return mapDataSourcePreparer.get(dataSource.getClass());
        }else{
            throw new UnsupportedDataSourceException(dataSource);
        }
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
            protected SparkDDF newInstance(String ds) {
                return newInstance(new SqlDataSource(ds));
            }

            @Override
            public SparkDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
 
