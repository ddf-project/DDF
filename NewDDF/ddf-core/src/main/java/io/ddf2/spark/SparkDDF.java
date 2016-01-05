package io.ddf2.spark;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.SqlDataSource;
import io.ddf2.datasource.filesystem.LocalFileDataSource;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.Schema;
import io.ddf2.spark.resolver.SparkLocalFilePreparer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * SparkDDF implement DDF using Spark & Hive
 * @see io.ddf2.IDDF
 */
public class SparkDDF extends DDF {

    protected Map<Class,IDataSourcePreparer> mapDataSourcePreparer;
    protected SparkContext sparkContext;
    protected HiveContext hiveContext;

    public static final String PROPERTY_HIVE_CONF = "HIVE_CONF";

    SparkDDF(IDataSource dataSource) {
        super(dataSource);
    }

    /**
     * @see DDF#build(Map)
     * @param mapDDFProperties required to contains JavaSparkContext
     */
    @Override
    protected void build(Map mapDDFProperties) throws PrepareDataSourceException {
        SparkContext sc = (SparkContext)mapDDFProperties.get("SparkContext");
        if(sc == null) throw new RuntimeException("SparkDDF required to have SparkContext On DdfProperties");
        this.sparkContext = sc;
        /* Init Hive Context & prepare data */
        this.hiveContext = new HiveContext(sc);
        Properties hiveProperties = null;
        if(mapDDFProperties.containsKey(PROPERTY_HIVE_CONF)){
            try {
                hiveProperties = (Properties) mapDDFProperties.get(PROPERTY_HIVE_CONF);
            }catch(Exception e){
                hiveProperties = null;
            }
        }
        if(hiveProperties == null){
            hiveProperties = new Properties();
            hiveProperties.setProperty("hive.metastore.warehouse.dir","/tmp/hive_warehouse");

        }
        this.hiveContext.setConf(hiveProperties);
        this.mapDDFProperties = mapDDFProperties;
        //all support datasource preparer
        mapDataSourcePreparer = new HashMap<>();
        mapDataSourcePreparer.put(LocalFileDataSource.class,new SparkLocalFilePreparer(hiveContext));
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

       return dataFrameToSqlResult(dataFrame);
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


    protected ISqlResult dataFrameToSqlResult(DataFrame df){
        return new SparkSqlResult(structTypeToSchema(df.schema()), new Iterable<Row>() {
            @Override
            public java.util.Iterator<Row> iterator() {
                return df.javaRDD().toLocalIterator();
            }
        });
    }
    protected ISchema structTypeToSchema(StructType structType){
        //ToDo convert Spark StructType to ISchema
        return new Schema();
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
            public SparkDDF build() throws DDFException {
                ddf.build(mapProperties);
                return ddf;
            }
        };
    }
}
 
