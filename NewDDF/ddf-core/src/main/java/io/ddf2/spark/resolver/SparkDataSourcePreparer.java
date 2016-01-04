package io.ddf2.spark.resolver;

import io.ddf2.datasource.IDataSource;
import io.ddf2.datasource.IDataSourcePreparer;
import io.ddf2.datasource.PrepareDataSourceException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 1/4/16.
 */
public abstract class SparkDataSourcePreparer implements IDataSourcePreparer {
    protected static Map<Class,String> mapJavaType2HiveType = new HashMap<>();
    static{
        mapJavaType2HiveType.put(String.class,"STRING");
        mapJavaType2HiveType.put(Integer.class,"INT");
        mapJavaType2HiveType.put(Long.class,"BIGINT");
        mapJavaType2HiveType.put(Float.class,"FLOAT");
        mapJavaType2HiveType.put(Double.class,"DOUBLE");
        mapJavaType2HiveType.put(Boolean.class,"BOOLEAN");
    }
    protected String getHiveType(Class javaType) throws PrepareDataSourceException {

        if(mapJavaType2HiveType.containsKey(javaType)){
            return mapJavaType2HiveType.get(javaType);
        }else{
            throw new PrepareDataSourceException("Unsupported Hive Type of " + javaType.getSimpleName());
        }
    }
}
