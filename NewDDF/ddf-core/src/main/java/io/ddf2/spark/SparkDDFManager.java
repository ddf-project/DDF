package io.ddf2.spark;

import io.ddf2.DDFException;
import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.datasource.IDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkDDFManager extends DDFManager {
    protected SparkConf sparkConf;
    protected SparkContext sparkContext;

    protected static AtomicInteger instanceCounter;
    protected final String ddfManagerId;

    public static final String PROPERTY_SPARK_CONF = "SparkConf";
    public static final String PROPERTY_SPARK_CONTEXT = "SparkContext";

    protected SparkDDFManager(Map properties) {
        super(properties);
        instanceCounter = new AtomicInteger();
        int instanceCount = instanceCounter.incrementAndGet();
        ddfManagerId = "SparkDDFManager_" + instanceCount;
        /* Init Spark */
        if (mapProperties.containsKey(PROPERTY_SPARK_CONF)) {
            sparkConf = (SparkConf) mapProperties.get(PROPERTY_SPARK_CONF);
        }
        if (sparkConf == null) {
            sparkConf = new SparkConf();
            sparkConf.setAppName(ddfManagerId);
            sparkConf.setMaster("local");
        }
        if (mapProperties.containsKey(PROPERTY_SPARK_CONTEXT)) {
            sparkContext = (SparkContext) mapProperties.get(PROPERTY_SPARK_CONTEXT);
        }
        if (sparkContext == null) {
            sparkContext = new SparkContext(sparkConf);
        }
        this.mapProperties.put("SparkContext", sparkContext);


    }

    @Override
    public IDDF newDDF(String name, IDataSource ds) throws DDFException {
        return newDDF(name, ds, mapProperties);
    }

    /**
     * @param ds
     * @see IDDFManager#newDDF(IDataSource)
     */
    @Override
    public IDDF newDDF(IDataSource ds) throws DDFException {
        return newDDF(generateDDFName(), ds, mapProperties);
    }


    @Override
    public String getDDFManagerId() {
        return ddfManagerId;
    }


    protected SparkDDF newDDF(String name, IDataSource dataSource, Map<String, Object> mapProperties) throws DDFException {
        return SparkDDF.builder(dataSource)
                .setName(name)
                .putProperty(mapProperties)
                .build();
    }

    protected synchronized String generateDDFName() {
        return String.format("ddf_%d_%ld_%d", instanceCounter.get(), System.currentTimeMillis(), System.nanoTime() % 10);
    }
}
 
