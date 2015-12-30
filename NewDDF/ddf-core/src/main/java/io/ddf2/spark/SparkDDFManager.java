package io.ddf2.spark;

import io.ddf2.DDFManager;
import io.ddf2.IDDF;
import io.ddf2.IDDFManager;
import io.ddf2.datasource.IDataSource;

import java.util.Map;

public class SparkDDFManager extends DDFManager {

    protected Map<String,Object> mapSparkProperties;

    protected SparkDDFManager(Map mapProperties) {
        super(mapProperties);
    }

    /**
     * @param ds
     * @see IDDFManager#newDDF(IDataSource)
     */
    @Override
    public IDDF newDDF(IDataSource ds) {
        return newDDF(ds,mapProperties);
    }

    /**
     * @param sql
     * @see IDDFManager#newDDF(String)
     */
    @Override
    public IDDF newDDF(String sql) {
        return null;
    }

    protected SparkDDF newDDF(IDataSource dataSource,Map<String,Object> mapProperties){
        return  SparkDDF.builder(dataSource)
                .build();
    }
}
 
