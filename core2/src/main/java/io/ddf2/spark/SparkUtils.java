package io.ddf2.spark;

import io.ddf2.ISqlResult;
import io.ddf2.datasource.PrepareDataSourceException;
import io.ddf2.datasource.schema.ISchema;
import io.ddf2.datasource.schema.Schema;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sangdn on 1/6/16.
 * <p>
 * SparkUtils provide some useful function when working with Spark
 * + Data Convert
 */
public class SparkUtils {
    /* using to parse JavaType from Schema to HiveQL Query (create table) */
    protected static Map<Class, String> mapJavaType2HiveType = new HashMap<>();
    /* using to parse HiveResult StructType to Schema */
    protected static Map<DataType, Class> mapSparkDataType2JavaType = new HashMap<>();

    static {

        mapJavaType2HiveType.put(Byte.class, "TINYINT");
        mapJavaType2HiveType.put(Short.class, "SMALLINT");
        mapJavaType2HiveType.put(Integer.class, "INT");
        mapJavaType2HiveType.put(Long.class, "BIGINT");
        mapJavaType2HiveType.put(String.class, "STRING");
        mapJavaType2HiveType.put(Float.class, "FLOAT");
        mapJavaType2HiveType.put(Double.class, "DOUBLE");
        mapJavaType2HiveType.put(TimeStamp.class, "TIMESTAMP");
        mapJavaType2HiveType.put(Date.class, "DATE");
        mapJavaType2HiveType.put(Boolean.class, "BOOLEAN");


        mapSparkDataType2JavaType.put( DataType.fromCaseClassString("ByteType"), Byte.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("ShortType"), Short.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("IntegerType"), Integer.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("LongType"), Long.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("FloatType"), Double.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("DoubleType"), Double.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("StringType"), String.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("TimestampType"), Timestamp.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("DateType"), Date.class);
        mapSparkDataType2JavaType.put(DataType.fromCaseClassString("BooleanType"), Boolean.class);



    }

    public static ISqlResult dataFrameToSqlResult(DataFrame df) {
        return new SparkSqlResult(structTypeToSchema(df.schema()), new Iterable<Row>() {
            @Override
            public java.util.Iterator<Row> iterator() {
                return df.javaRDD().toLocalIterator();
            }
        });
    }

    public static ISchema structTypeToSchema(StructType structType) {
        Schema.SchemaBuilder schemaBuilder = Schema.builder();
        for (StructField field : structType.fields()) {
            schemaBuilder.add(field.name(), SparkUtils.mapSparkDataType2JavaType.get(field.dataType()));
        }
        return schemaBuilder.build();
    }



    public static String javaTypeToHiveName(Class javaType) throws PrepareDataSourceException {

        if (mapJavaType2HiveType.containsKey(javaType)) {
            return mapJavaType2HiveType.get(javaType);
        } else {
            throw new PrepareDataSourceException("Unsupported Hive Type of " + javaType.getSimpleName());
        }
    }
}
