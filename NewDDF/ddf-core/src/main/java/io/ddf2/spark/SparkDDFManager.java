package io.ddf2.spark;

import io.ddf2.*;
import io.ddf2.datasource.IDataSource;
import io.ddf2.handlers.IPersistentHandler;
import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkDDFManager extends DDFManager {
    protected SparkConf sparkConf;
    protected SparkContext sparkContext;
    protected HiveContext hiveContext;



    public static final String KEY_SPARK_CONF = "SPARK-CONFIG";
    public static final String KEY_SPARK_CONTEXT = "SPARK-CONTEXT";
    public static final String KEY_HIVE_CONTEXT = "HIVE-CONTEXT";
    public static final String KEY_HIVE_PROPERTIES = "HIVE-PROPERTIES";


    protected SparkDDFManager(Map options) {
        super(options);

        /* Init Spark */

        if (mapProperties.containsKey(KEY_SPARK_CONTEXT)) {
            sparkContext = (SparkContext) mapProperties.get(KEY_SPARK_CONTEXT);
        }
        if (sparkContext == null) {
            if (mapProperties.containsKey(KEY_SPARK_CONF)) {
                sparkConf = (SparkConf) mapProperties.get(KEY_SPARK_CONF);
            }
            if (sparkConf == null) {
                sparkConf = new SparkConf();
                sparkConf.setAppName("SparkDDFManager");
                sparkConf.setMaster("local");
            }
            sparkContext = new SparkContext(sparkConf);
        }


        /* Init Hive Context*/
        if(mapProperties.containsKey(KEY_HIVE_CONTEXT)){
            this.hiveContext = (HiveContext) mapProperties.get(KEY_HIVE_CONTEXT);
        }else {
            this.hiveContext = new HiveContext(sparkContext);
        }
        Properties hiveProperties = null;
        if(mapProperties.containsKey(KEY_HIVE_PROPERTIES)){
            try {
                hiveProperties = (Properties) mapProperties.get(KEY_HIVE_PROPERTIES);
                this.hiveContext.setConf(hiveProperties);
            }catch(Exception e){
                hiveProperties = null;
            }
        }

        /*Init Properties to pass to SparkDDF*/
        mapProperties.put(SparkDDF.PROPERTY_HIVE_CONTEXT,this.hiveContext);
        mapProperties.put(SparkDDF.PROPERTY_SPARK_CONTEXT,this.sparkContext);

    }

    @Override
    protected IDDF _newDDF(String name, IDataSource ds) throws DDFException {
        return newDDF(name, ds, mapProperties);
    }
    @Override
    public IDDF newDDF(String query) throws DDFException {
        return null;
    }

    @Override
    public IDDF newDDF(String name, String query) throws DDFException {
        return null;
    }

    @Override
    public IDDF newDDF(String query, Map<String, String> options) {
        return null;
    }

    @Override
    public IDDF newDDF(String name, String query, Map<String, String> options) {
        return null;
    }


    @Override
    protected IPersistentHandler _getPersistentHanlder() {
        throw new NotImplementedException("Not Implement Yet");
    }

    @Override
    protected IDDFMetaData _getDDFMetaData() {

        return new SparkDDFMetadata(hiveContext);
    }



    @Override
    public ISqlResult sql(String query) {
        DataFrame dataFrame = hiveContext.sql(query);
        return SparkUtils.dataFrameToSqlResult(dataFrame);
    }

    @Override
    public ISqlResult sql(String query, Map<String, String> options) throws SQLException {
        return null;
    }


    protected SparkDDF newDDF(String name, IDataSource dataSource, Map<String, Object> mapProperties) throws DDFException {
        return SparkDDF.builder(dataSource)
                .setName(name)
                .putProperty(mapProperties)
                .build();
    }

}
 
