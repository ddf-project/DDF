/**
 *
 */
package io.ddf.misc;


import com.google.common.base.Strings;
import io.ddf.util.ConfigHandler;
import io.ddf.util.ConfigHandler.Configuration;
import io.ddf.util.IHandleConfig;
import io.ddf.util.Utils;

import java.io.IOException;


/**
 * Groups all the configuration-related code that would otherwise crowd up DDF.java
 */
public class Config {

  /**
   * Returns the runtime basic-storage directory path name, creating one if necessary.
   *
   * @return
   * @throws IOException
   */
  public static String getRuntimeDir() throws IOException {
    return Utils.locateOrCreateDirectory(getGlobalValue(ConfigConstant.FIELD_RUNTIME_DIR));
  }

  public static String getBasicPersistenceDir() throws IOException {
    return String.format("%s/%s", getRuntimeDir(), getGlobalValue(ConfigConstant.FIELD_BASIC_PERSISTENCE_DIRECTORY));
  }


  public static String getValue(ConfigConstant section, ConfigConstant key) {
    return getValue(section.toString(), key.toString());
  }

  public static String getValue(String section, ConfigConstant key) {
    return getValue(section, key.toString());
  }

  public static String getValue(String section, String key) {
    return getConfigHandler().getValue(section, key);
  }


  /**
   * If the named section does not have the value, then try the same key from the "global" section
   *
   * @param section
   * @param key
   * @return
   */
  public static String getValueWithGlobalDefault(ConfigConstant section, ConfigConstant key) {
    String value = getValue(section, key);
    return (Strings.isNullOrEmpty(value) ? getGlobalValue(key) : value);
  }

  /**
   * If the named section does not have the value, then try the same key from the "global" section
   *
   * @param section
   * @param key
   * @return
   */
  public static String getValueWithGlobalDefault(String section, ConfigConstant key) {
    String value = getValue(section, key);
    return (Strings.isNullOrEmpty(value) ? getGlobalValue(key) : value);
  }

  /**
   * If the named section does not have the value, then try the same key from the "global" section
   *
   * @param section
   * @param key
   * @return
   */
  public static String getValueWithGlobalDefault(String section, String key) {
    String value = getValue(section, key);
    return (Strings.isNullOrEmpty(value) ? getGlobalValue(key) : value);
  }


  public static String getGlobalValue(ConfigConstant key) {
    return getValue(ConfigConstant.SECTION_GLOBAL.toString(), key.toString());
  }

  public static String getGlobalValue(String key) {
    return getValue(ConfigConstant.SECTION_GLOBAL.toString(), key);
  }


  public static void set(String section, String key, String value) {
    getConfigHandler().getSection(section).set(key, value);
  }


  static IHandleConfig sConfigHandler;


  public static IHandleConfig getConfigHandler() {
    if (sConfigHandler == null) {
      String configFileName = System.getenv(ConfigConstant.DDF_INI_ENV_VAR.toString());
      if (Strings.isNullOrEmpty(configFileName)) configFileName = ConfigConstant.DDF_INI_FILE_NAME.toString();
      sConfigHandler = new ConfigHandler(ConfigConstant.DDF_CONFIG_DIR.toString(), configFileName);

      if (sConfigHandler.getConfig() == null) {
        // HACK: prep a basic default config!
        Configuration config = new Configuration();

        config.getSection(ConfigConstant.SECTION_GLOBAL.toString()) //
            .set("Namespace", "adatao") //
            .set("RuntimeDir", "ddf-runtime") //
            .set("BasicPersistenceDir", "basic-ddf-db") //
            .set("DDF", "io.ddf.DDF") //
            .set("io.ddf.DDF", "io.ddf.DDFManager") //
            .set("ISupportStatistics", "io.ddf.analytics.AStatisticsSupporter") //
            .set("IHandleRepresentations", "io.ddf.content.RepresentationHandler") //
            .set("IHandleSchema", "io.ddf.content.SchemaHandler") //
            .set("IHandleViews", "io.ddf.content.ViewHandler") //
            .set("IHandlePersistence", "io.basic.ddf.content.PersistenceHandler") //
            .set("IHandleMetaData", "io.ddf.content.MetaDataHandler") //
        ;

        config.getSection("basic") //
            .set("DDF", "io.basic.ddf.BasicDDF") //
            .set("DDFManager", "io.basic.ddf.BasicDDFManager") //
        ;

        config.getSection("spark") //
            .set("DDF", "io.ddf.spark.SparkDDF") //
            .set("DDFManager", "io.ddf.spark.SparkDDFManager") //
            .set("ISupportStatistics", "io.ddf.spark.analytics.BasicStatisticsSupporter") //
            .set("IHandleMetaData", "io.ddf.spark.content.MetaDataHandler") //
            .set("IHandleRepresentations", "io.ddf.spark.content.RepresentationHandler") //
            .set("IHandleSchema", "io.ddf.spark.content.SchemaHandler") //
            .set("IHandleSql", "io.ddf.spark.etl.SqlHandler") //
            .set("IHandleViews", "io.ddf.spark.content.ViewHandler") //
            .set("ISupportML", "io.ddf.spark.ml.MLSupporter") //
        ;

        sConfigHandler.setConfig(config);
      }
    }

    return sConfigHandler;
  }


  /**
   * Common constants that all clients can/should use, to avoid spelling errors
   */
  public enum ConfigConstant {
    // @formatter:off
    
    DDF_INI_ENV_VAR("DDF_INI"), DDF_INI_FILE_NAME("ddf.ini"), DDF_CONFIG_DIR("ddf-conf"),
    
    ENGINE_NAME_DEFAULT("spark"), ENGINE_NAME_BASIC("basic"), ENGINE_NAME_SPARK("spark"),
      ENGINE_NAME_SQLLITE("sqllite"), ENGINE_NAME_JDBC("jdbc"),
    
    SECTION_GLOBAL("global"), 
    
    FIELD_RUNTIME_DIR("RuntimeDir"), FIELD_NAMESPACE("Namespace"), FIELD_DDF("DDF"), FIELD_DDF_MANAGER("DDFManager"),
    FIELD_BASIC_PERSISTENCE_DIRECTORY("BasicPersistenceDir")
    
    ;
    // @formatter:on

    private String mValue;


    private ConfigConstant(String value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return mValue;
    }
  }
}
