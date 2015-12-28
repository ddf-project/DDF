package io.newddf.spark;

import io.ddf.analytics.AggregationHandler;
import io.newddf.DDF;
import io.newddf.DDFManager;
import io.newddf.IDataSource;

import java.util.Collections;
import java.util.Map;

/**
 * Created by sangdn on 12/21/15.
 */
public class SparkDDFManager extends DDFManager {
    private Map<String,Object> sparkProperties = Collections.emptyMap();
    protected SparkDDFManager(Map params) {
        super(params);
    }

    @Override
    public SparkDDF newDDF(IDataSource ds) {
        return newDDF(ds,sparkProperties);
    }
    protected SparkDDF newDDF(IDataSource dataSource,Map<String,Object> mapProperties){
        return  SparkDDF.builder(dataSource).setAggregationHandler(null)
                .setBinningHandler(null)
                .setIndexingHandler(null)
                .setJoinsHandler(null)
                .setMetaDataHandler(null)
                .setMiscellanyHandler(null)
                .setMissingDataHandler(null)
                .setMLMetricsSupporter(null)
                .setMutabilityHandler(null)
                .setPersistenceHandler(null)
                .setRepresentationHandler(null)
                .setReshapingHandler(null)
                .setSchemaHandler(null)
                .setSqlHandler(null)
                .setStatisticsSupporter(null)
                .setStreamingDataHandler(null)
                .setTimeSeriesHandler(null)
                .setTransformationHandler(null)
                .setViewHandler(null)
                .build();
    }
}
