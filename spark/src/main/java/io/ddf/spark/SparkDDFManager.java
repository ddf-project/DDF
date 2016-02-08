package io.ddf.spark;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.datasource.DataSourceDescriptor;
import io.ddf.datasource.JDBCDataSourceDescriptor;
import io.ddf.ds.DataSourceCredential;
import io.ddf.exception.DDFException;
import io.ddf.spark.ds.DataSource;
import io.ddf.spark.etl.DateParseUDF;
import io.ddf.spark.etl.DateTimeExtractUDF;
import io.ddf.spark.etl.DateUDF;
import io.ddf.spark.util.SparkUtils;
import io.ddf.spark.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.File;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.*;

//import shark.SharkEnv;
//import shark.api.JavaSharkContext;

/**
 * An Apache-Spark-based implementation of DDFManager
 */
public class SparkDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "spark";
  }


  private static final String DEFAULT_SPARK_APPNAME = "DDFClient";
  private static final String DEFAULT_SPARK_MASTER = "local[4]";

  public SparkDDFManager(SparkContext sparkContext) throws DDFException {
    this.setEngineType(EngineType.SPARK);
    this.initialize(sparkContext, null);
  }

  @Override
  public DDF transferByTable(UUID fromEngine, String tableName) throws
          DDFException {

    mLog.info("Get the engine " + fromEngine + " to transfer table : " +
            tableName);
    DDFManager fromManager = this.getDDFCoordinator().getEngine(fromEngine);
    DataSourceDescriptor dataSourceDescriptor = fromManager
            .getDataSourceDescriptor();
    if (dataSourceDescriptor instanceof JDBCDataSourceDescriptor) {
      // JDBCConnection.
      JDBCDataSourceDescriptor jdbcDS = (JDBCDataSourceDescriptor)
              dataSourceDescriptor;

      if (fromManager.getEngine().equalsIgnoreCase("sfdc")) {
        try {
          JDBCDataSourceDescriptor sfdcDS = new JDBCDataSourceDescriptor
                  (jdbcDS.getDataSourceUri().getUri().toString(),
                   "",
                   "",
                   tableName);
          return this.load(sfdcDS);
        } catch (URISyntaxException e) {
          throw new DDFException(e);
        }
      } else {
        JDBCDataSourceDescriptor loadDS
                = new JDBCDataSourceDescriptor(jdbcDS.getDataSourceUri(),
                                               jdbcDS.getCredentials(),
                                               tableName);
        mLog.info("load from JDBCDatasource, " + loadDS.getDataSourceUri()
                .getUri().toString() + ", " + loadDS.getCredentials()
                .getUsername() + ", " + loadDS.getCredentials().getPassword()
                + ", " + loadDS.getDbTable());
        return this.load(loadDS);
      }
      // TODO
      /*
      if (fromManager.getEngine().equals("sfdc")) {
        options.put("url", jdbcDataSourceDescriptor.getDataSourceUri().getUri
                ().toString());
        mLog.info("sfdc uri: " + jdbcDataSourceDescriptor.getDataSourceUri()
                .getUri().toString());
      } else {
        options.put("url", jdbcDataSourceDescriptor.getDataSourceUri().getUri()
                .toString() + "?user=" + jdbcCredential.getUsername() +
                "&password="+jdbcCredential.getPassword());

      }


      // TODO: Pay attention here. Some maybe username?
      // options.put("user", jdbcCredential.getUserName());
      // options.put("password", jdbcCredential.getPassword());
      // TODO: What if sfdc.
      DataFrame rdd = mHiveContext.load("jdbc", options);
      if (rdd == null) {
        throw new DDFException("fail use spark datasource api");
      }
      Schema schema = SchemaHandler.getSchemaFromDataFrame(rdd);
      DDF ddf = this.newDDF(this, rdd, new Class<?>[]
              {DataFrame.class}, null, null, null, schema);
      ddf.getRepresentationHandler().get(new Class<?>[]{RDD.class, Row.class});
      */
    } else {
      throw new DDFException("Currently no other DataSourceDescriptor is " +
              "supported");
    }
  }

  @Override
    public DDF transfer(UUID fromEngine, UUID ddfUUID) throws DDFException {
    DDFManager fromManager = this.getDDFCoordinator().getEngine(fromEngine);
    mLog.info("Get the engine " + fromEngine + " to transfer ddf : " +
        ddfUUID.toString());
    DDF fromDDF = fromManager.getDDF(ddfUUID);
    if (fromDDF == null) {
      throw new DDFException("There is no ddf with uri : " + ddfUUID.toString()
          + " in another engine");
    }
    String fromTableName = fromDDF.getTableName();
    return this.transferByTable(fromEngine, fromTableName);
  }

  /**
   * Use system environment variables to configure the SparkContext creation.
   *
   * @throws DDFException
   */
  public SparkDDFManager() throws DDFException {
    this.setEngineType(EngineType.SPARK);
    this.initialize(null, new HashMap<String, String>());
  }

  public SparkDDFManager(Map<String, String> params) throws DDFException {
    this.setEngineType(EngineType.SPARK);
    this.initialize(null, params);
  }

  private void initialize(SparkContext sparkContext, Map<String, String> params) throws DDFException {
    this.setSparkContext(sparkContext == null ? this.createSparkContext(params) : sparkContext);
    this.mHiveContext = new HiveContext(this.mSparkContext);
    String compression = System.getProperty("spark.sql.inMemoryColumnarStorage.compressed", "true");
    String batchSize = System.getProperty("spark.sql.inMemoryColumnarStorage.batchSize", "1000");
    mLog.info(">>>> spark.sql.inMemoryColumnarStorage.compressed= " + compression);
    mLog.info(">>>> spark.sql.inMemoryColumnarStorage.batchSize= " + batchSize);
    this.mHiveContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", compression);
    this.mHiveContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", batchSize);

    // register SparkSQL UDFs
    this.registerUDFs();
  }
  // TODO: Dynamically load UDFs
  private void registerUDFs() {
    DateParseUDF.register(this.mHiveContext);
    DateTimeExtractUDF.register(this.mHiveContext);
    DateUDF.registerUDFs(this.mHiveContext);
  }



  public String getDDFEngine() {
    return "spark";
  }


  private SparkContext mSparkContext;

  private JavaSparkContext mJavaSparkContext;

  public SparkContext getSparkContext() {
    return mSparkContext;
  }

  private void setSparkContext(SparkContext sparkContext) {
    this.mSparkContext = sparkContext;
  }

  private HiveContext mHiveContext;

  private static final String[][] SPARK_ENV_VARS = new String[][] {
    // @formatter:off
    { "SPARK_APPNAME", "spark.appname" },
    { "SPARK_MASTER", "spark.master" },
    { "SPARK_HOME", "spark.home" },
    { "SPARK_SERIALIZER", "spark.kryo.registrator" },
    { "HIVE_HOME", "hive.home" },
    { "HADOOP_HOME", "hadoop.home" },
    { "DDFSPARK_JAR", "ddfspark.jar" }
    // @formatter:on
  };

  public HiveContext getHiveContext() {
    return mHiveContext;
  }
  //  private SparkUtils.createSharkContext mSharkContext;
  //
  //
  //  public SharkContext getSharkContext() {
  //    return mSharkContext;
  //  }

  //  private JavaSharkContext mJavaSharkContext;
  //
  //
  //  public JavaSharkContext getJavaSharkContext() {
  //    return mJavaSharkContext;
  //  }

  //  public void setJavaSharkContext(JavaSharkContext javaSharkContext) {
  //    this.mJavaSharkContext = javaSharkContext;
  //  }

  /**
   * Also calls setSparkContext() to the same sharkContext
   *
   * @param sharkContext
   */
  //  private void setSharkContext(SharkContext sharkContext) {
  //    this.mSharkContext = sharkContext;
  //    this.setSparkContext(sharkContext);
  //  }


  private Map<String, String> mSparkContextParams;


  public Map<String, String> getSparkContextParams() {
    return mSparkContextParams;
  }

  private void setSparkContextParams(Map<String, String> mSparkContextParams) {
    this.mSparkContextParams = mSparkContextParams;
  }
   /* merge priority is as follows: (1) already set in params, (2) in system properties (e.g., -Dspark.home=xxx), (3) in
   * environment variables (e.g., export SPARK_HOME=xxx)
   *
   * @param params
   * @return
   */
  private Map<String, String> mergeSparkParamsFromSettings(Map<String, String> params) {
    if (params == null) params = new HashMap<String, String>();

    Map<String, String> env = System.getenv();

    for (String[] varPair : SPARK_ENV_VARS) {
      if (params.containsKey(varPair[0])) continue; // already set in params

      // Read setting either from System Properties, or environment variable.
      // Env variable has lower priority if both are set.
      String value = System.getProperty(varPair[1], env.get(varPair[0]));
      if (value != null && value.length() > 0) params.put(varPair[0], value);
    }

    // Some well-known defaults
    if (!params.containsKey("SPARK_MASTER")) params.put("SPARK_MASTER", DEFAULT_SPARK_MASTER);
    if (!params.containsKey("SPARK_APPNAME")) params.put("SPARK_APPNAME", DEFAULT_SPARK_APPNAME);
    params.put("SPARK_SERIALIZER", "io.spark.content.KryoRegistrator");
    Gson gson = new Gson();

    mLog.info(String.format(">>>>>>> params = %s", gson.toJson(params)));

    return params;
  }

  /**
   * Side effect: also sets SharkContext and SparkContextParams in case the client wants to examine or use those.
   *
   * @param params
   * @return
   * @throws DDFException
   */
  private SparkContext createSparkContext(Map<String, String> params) throws DDFException {
    this.setSparkContextParams(this.mergeSparkParamsFromSettings(params));
    String ddfSparkJar = params.get("DDFSPARK_JAR");

    String[] jobJars = ddfSparkJar != null ? ddfSparkJar.split(",") : new String[] { };

    ArrayList<String> finalJobJars = new ArrayList<String>();

    // DDFSPARK_JAR could contain directories too, used in case of Databricks Cloud where we don't have the jars at start-up
    // time but the directory and we will search and includes all of jars at run-time
    for (String jobJar: jobJars) {
      if ((!jobJar.endsWith(".jar")) && (new File(jobJar).list() != null)) {
        // This is a directory
        ArrayList<String> jars = Utils.listJars(jobJar);
        if (jars != null) {
          finalJobJars.addAll(jars);
        }

      } else  {
        finalJobJars.add(jobJar);
      }
    }

    mLog.info(">>>>> ddfSparkJar = " + ddfSparkJar);
    for(String jar: finalJobJars) {
      mLog.info(">>>>> " + jar);
    }

    jobJars = new String[finalJobJars.size()];
    jobJars = finalJobJars.toArray(jobJars);

    for (String key : params.keySet()) {
      mLog.info(">>>> key = " + key + ", value = " + params.get(key));
    }

    SparkContext context = SparkUtils.createSparkContext(params.get("SPARK_MASTER"), params.get("SPARK_APPNAME"),
        params.get("SPARK_HOME"), jobJars, params);
    this.mSparkContext = context;
    this.mJavaSparkContext = new JavaSparkContext(context);
    return this.getSparkContext();
  }

  @Override
  public DDF newDDF(DDFManager manager, Object data, Class<?>[] typeSpecs,
      String namespace, String name, Schema
                              schema)
          throws DDFException {
    DDF ddf = super.newDDF(manager, data, typeSpecs, namespace,
            name, schema);
    ((SparkDDF) ddf).saveAsTable();
    return ddf;
  }

  @Override
  public DDF newDDF(Object data, Class<?>[] typeSpecs,
                    String namespace, String name, Schema schema)
          throws DDFException {
    DDF ddf = super.newDDF(data, typeSpecs, namespace, name,
            schema);
    ((SparkDDF) ddf).saveAsTable();
    return ddf;
  }

  public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
    JavaRDD<String> fileRDD = mJavaSparkContext.textFile(fileURL);
    String[] metaInfos = getMetaInfo(fileRDD, fieldSeparator);
    SecureRandom rand = new SecureRandom();
    String tableName = "tbl" + String.valueOf(Math.abs(rand.nextLong()));
    String cmd = "CREATE TABLE " + tableName + "(" + StringUtils.join(metaInfos, ", ")
        + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fieldSeparator + "'";
    sql(cmd, "SparkSQL");
    sql("LOAD DATA LOCAL INPATH '" + fileURL + "' " +
        "INTO TABLE " + tableName, "SparkSQL");
    return sql2ddf("SELECT * FROM " + tableName, "SparkSQL");
  }

  @Override
  public DDF getOrRestoreDDFUri(String ddfURI) throws DDFException {
    return this.mDDFCache.getDDFByUri(ddfURI);
  }

  @Override
  public DDF getOrRestoreDDF(UUID uuid) throws DDFException {
    return this.mDDFCache.getDDF(uuid);
  }

  @Override
  public DDF createDDF(Map<Object, Object> options) throws DDFException {
    Preconditions.checkArgument(options.containsKey("dataSource"),
        "SparkDDFManager need dataSource param in options");

    Object dsObject = options.get("dataSource");
    Preconditions.checkArgument(dsObject instanceof DataSource,
        "dataSource option is not of DataSource type");

    DataSource ds = (DataSource) dsObject;
    return ds.loadDDF(options);
  }

  @Override
  public void validateCredential(DataSourceCredential credential) throws DDFException {
    // no credential needed for spark, do nothing here
  }

  @Override
  public String getSourceUri() {
    return "spark:;";
  }

  /**
   * Given a String[] vector of data values along one column, try to infer what the data type should be.
   * <p/>
   * TODO: precompile regex
   *
   * @param vector
   * @return string representing name of the type "integer", "double", "character", or "logical" The algorithm will
   * first scan the vector to detect whether the vector contains only digits, ',' and '.', <br>
   * if true, then it will detect whether the vector contains '.', <br>
   * &nbsp; &nbsp; if true then the vector is double else it is integer <br>
   * if false, then it will detect whether the vector contains only 'T' and 'F' <br>
   * &nbsp; &nbsp; if true then the vector is logical, otherwise it is characters
   */
  public static String determineType(String[] vector, Boolean doPreferDouble) {
    boolean isNumber = true;
    boolean isInteger = true;
    boolean isLogical = true;
    boolean allNA = true;

    for (String s : vector) {
      if (s == null || s.startsWith("NA") || s.startsWith("Na") || s.matches("^\\s*$")) {
        // Ignore, don't set the type based on this
        continue;
      }

      allNA = false;

      if (isNumber) {
        // match numbers: 123,456.123 123 123,456 456.123 .123
        if (!s.matches("(^|^-)((\\d+(,\\d+)*)|(\\d*))\\.?\\d+$")) {
          isNumber = false;
        }
        // match double
        else if (isInteger && s.matches("(^|^-)\\d*\\.{1}\\d+$")) {
          isInteger = false;
        }
      }

      // NOTE: cannot use "else" because isNumber changed in the previous
      // if block
      if (isLogical && !s.toLowerCase().matches("^t|f|true|false$")) {
        isLogical = false;
      }
    }

    // String result = "Unknown";
    String result = "string";

    if (!allNA) {
      if (isNumber) {
        if (!isInteger || doPreferDouble) {
          result = "double";
        } else {
          result = "int";
        }
      } else {
        if (isLogical) {
          result = "boolean";
        } else {
          result = "string";
        }
      }
    }
    return result;
  }

  /**
   * TODO: check more than a few lines in case some lines have NA
   *
   * @param fileRDD
   * @return
   */
  public String[] getMetaInfo(JavaRDD<String> fileRDD, String fieldSeparator) {
    String[] headers = null;
    int sampleSize = 5;

    // sanity check
    if (sampleSize < 1) {
      mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1");
      return null;
    }

    List<String> sampleStr = fileRDD.take(sampleSize);
    sampleSize = sampleStr.size(); // actual sample size
    mLog.info("Sample size: " + sampleSize);

    // create sample list for getting data type
    String[] firstSplit = sampleStr.get(0).split(fieldSeparator);

    // get header
    boolean hasHeader = false;
    if (hasHeader) {
      headers = firstSplit;
    } else {
      headers = new String[firstSplit.length];
      int size = headers.length;
      for (int i = 0; i < size; ) {
        headers[i] = "V" + (++i);
      }
    }

    String[][] samples = hasHeader ? (new String[firstSplit.length][sampleSize - 1])
        : (new String[firstSplit.length][sampleSize]);

    String[] metaInfoArray = new String[firstSplit.length];
    int start = hasHeader ? 1 : 0;
    for (int j = start; j < sampleSize; j++) {
      firstSplit = sampleStr.get(j).split(fieldSeparator);
      for (int i = 0; i < firstSplit.length; i++) {
        samples[i][j - start] = firstSplit[i];
      }
    }

    boolean doPreferDouble = true;
    for (int i = 0; i < samples.length; i++) {
      String[] vector = samples[i];
      metaInfoArray[i] = headers[i] + " " + determineType(vector, doPreferDouble);
    }

    return metaInfoArray;
  }
}  

