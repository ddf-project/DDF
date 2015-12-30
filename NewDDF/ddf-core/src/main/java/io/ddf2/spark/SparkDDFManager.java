package io.ddf2.spark;

import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.datasource.IDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkDDFManager extends DDFManager {
    protected final SparkConf sparkConf;
    protected final JavaSparkContext javaSparkContext;
    protected final Map mapDDFProperties;
    protected static AtomicInteger instanceCounter;
    protected final String ddfManagerId;
    protected SparkDDFManager(Map mapProperties) {
        super(mapProperties);
        int instanceCount =instanceCounter.incrementAndGet();
        ddfManagerId = "SparkDDFManager_" + instanceCount;
        /* Init Spark */
        sparkConf = new SparkConf();
        sparkConf.setAppName(ddfManagerId);
        sparkConf.setMaster("local");
        javaSparkContext = new JavaSparkContext(sparkConf);
        mapDDFProperties = new HashMap();
        mapDDFProperties.put("JavaSparkContext",javaSparkContext);
        mapDDFProperties.put("ManagerId",ddfManagerId);

    }

    /**
     * @param ds
     * @see IDDFManager#newDDF(IDataSource)
     */
    @Override
    public IDDF newDDF(IDataSource ds) {
        return newDDF(ds,mapProperties);
    }

    @Override
    public String getDDFManagerId() {
        return ddfManagerId;
    }


    protected SparkDDF newDDF(IDataSource dataSource,Map<String,Object> mapProperties){
        return  SparkDDF.builder(dataSource)
                .putProperty(mapProperties)
                .build();
    }
}
 
