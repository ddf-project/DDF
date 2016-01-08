package io.ddf2.spark;

import io.ddf2.DDFManager;
import io.ddf2.IDDFManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import utils.TestUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 1/8/16.
 */
public class SparkTestUtils extends TestUtils {
    public static final SparkConf sparkConf = new SparkConf();
    public static final SparkContext sparkContext;
    public static final HiveContext hiveContext;
    public static final SparkDDFManager ddfManager;

    static {

        sparkConf.setAppName("SparkTest");
        sparkConf.setMaster("local");
        sparkConf.set(" spark.driver.allowMultipleContexts", "true");

        sparkContext = new SparkContext(sparkConf);
        hiveContext = new HiveContext(sparkContext);

        Map<String, Object> options = new HashMap<>();
        options.put(SparkDDFManager.KEY_SPARK_CONTEXT, sparkContext);
        options.put(SparkDDFManager.KEY_HIVE_CONTEXT, hiveContext);
        ddfManager = DDFManager.getInstance(SparkDDFManager.class, options);
    }

    public static HiveContext getHiveContext() {
        return hiveContext;
    }

    public static SparkContext getSparkContext() {
        return sparkContext;
    }

    public static SparkDDFManager getSparkDDFManager() {
        return ddfManager;
    }

    public static boolean makeParquetFileUserInfo(HiveContext hiveContext, String fileName, int numOfLine) {
        String jsonTempData = "/tmp/ + " + genString() + ".json";
        assert makeJSONFileUserInfo(jsonTempData, numOfLine) == true;

        DataFrame load = hiveContext.read().json(jsonTempData);
        deleteFile(fileName);
        load.saveAsParquetFile(fileName);
        deleteFile(jsonTempData);
        return true;
    }
}
