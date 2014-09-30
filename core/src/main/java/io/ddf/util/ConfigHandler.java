/**
 * 
 */
package io.ddf.util;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import io.ddf.misc.ALoggable;
import io.ddf.util.ConfigHandler.Configuration.Section;
import com.google.common.base.Strings;


/**
 * @author ctn
 * 
 */
public class ConfigHandler extends ALoggable implements IHandleConfig {

  public ConfigHandler(String configDir, String configFileName) {
    mConfigDir = configDir;
    mConfigFileName = configFileName;
  }


  private String mConfigDir;
  private String mConfigFileName;


  public String getConfigDir() {
    return mConfigDir;
  }

  public String getConfigFileName() {
    return mConfigFileName;
  }


  private Configuration mConfig;


  @Override
  public Configuration getConfig() {
    if (mConfig == null) try {
      mConfig = this.loadConfig();

    } catch (Exception e) {
      mLog.error("Unable to initialize configuration", e);
    }

    return mConfig;
  }


  /**
   * Stores DDF configuration information from ddf.ini
   */
  public static class Configuration {

    public static class Section {
      private Map<String, String> mEntries = new HashMap<String, String>();


      public Map<String, String> getEntries() {
        return mEntries;
      }

      public String get(String key) {
        return mEntries.get(safeToLower(key));
      }

      public Section set(String key, String value) {
        mEntries.put(safeToLower(key), value);
        return this;
      }

      public void remove(String key) {
        mEntries.remove(safeToLower(key));
      }

      public void clear() {
        mEntries.clear();
      }
    }


    private Map<String, Section> mSections;


    public Configuration() {
      this.reset();
    }

    private static String safeToLower(String s) {
      return s == null ? null : s.toLowerCase();
    }

    public Map<String, Section> getSections() {
      return mSections;
    }

    public Map<String, String> getSettings(String sectionName) {
      Section section = this.getSection(sectionName);
      return section.getEntries();
    }

    public Section getSection(String sectionName) {
      if (mSections == null) return null;

      Section section = mSections.get(safeToLower(sectionName));

      if (section == null) {
        section = new Section();
        mSections.put(safeToLower(sectionName), section);
      }

      return section;
    }

    public void removeSection(String sectionName) {
      if (mSections == null) return;

      this.getSection(sectionName).clear();
      mSections.remove(safeToLower(sectionName));
    }

    public void reset() {
      if (mSections != null) {
        for (Section section : mSections.values()) {
          section.clear();
        }
      }
      mSections = new HashMap<String, Section>();
    }
  }


  /**
   * Load configuration from ddf.ini, or the file name specified by the environment variable DDF_INI.
   * 
   * @throws Exception
   * 
   * @return the {@link Configuration} object loaded
   * @throws ConfigurationException
   *           , {@link IOException}
   */
  @Override
  public Configuration loadConfig() throws ConfigurationException, IOException {

    Configuration resultConfig = new Configuration();

    if (!Utils.localFileExists(this.getConfigFileName())) {
      // String configFileName = System.getenv(ConfigConstant.DDF_INI_ENV_VAR.getValue());
      File file = new File(this.locateConfigFileName(this.getConfigDir(), this.getConfigFileName()));
      mConfigDir = file.getParentFile().getName();
      mConfigFileName = file.getCanonicalPath();
    }

    if (!Utils.localFileExists(this.getConfigFileName())) return null;

    HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(this.getConfigFileName());

    @SuppressWarnings("unchecked")
    Set<String> sectionNames = config.getSections();
    for (String sectionName : sectionNames) {
      SubnodeConfiguration section = config.getSection(sectionName);
      if (section != null) {
        Configuration.Section resultSection = resultConfig.getSection(sectionName);

        @SuppressWarnings("unchecked")
        Iterator<String> keys = section.getKeys();
        while (keys.hasNext()) {
          String key = keys.next();
          String value = section.getString(key);
          if (value != null) {
            resultSection.set(key, value);
          }
        }
      }
    }

    mConfig = resultConfig;
    return mConfig;
  }

  @Override
  public void setConfig(Configuration theConfig) {
    mConfig = theConfig;
  }

  /**
   * Search in current dir and working up, looking for the config file
   * 
   * @return
   * @throws IOException
   */
  private String locateConfigFileName(String configDir, String configFileName) throws IOException {
    String curDir = new File(".").getCanonicalPath();

    String path = null;

    // Go for at most 10 levels up
    for (int i = 0; i < 10; i++) {
      path = String.format("%s/%s", curDir, configFileName);
      if (Utils.localFileExists(path)) break;

      String dir = String.format("%s/%s", curDir, configDir);
      if (Utils.dirExists(dir)) {
        path = String.format("%s/%s", dir, configFileName);
        if (Utils.localFileExists(path)) break;
      }

      curDir = String.format("%s/..", curDir);
    }

    mSource = path;

    if (Strings.isNullOrEmpty(path)) throw new IOException(String.format("Cannot locate DDF configuration file %s",
        configFileName));

    mLog.debug(String.format("Using config file found at %s\n", path));
    return path;
  }

  public Section getSection(String sectionName) {
    return this.getConfig().getSection(sectionName);
  }

  @Override
  public String getValue(String sectionName, String key) {
    Section section = this.getConfig().getSection(sectionName);
    return section == null ? null : section.get(key);
  }

  @Override
  public Map<String, Section> getSections() {
    return this.getConfig().getSections();
  }

  @Override
  public Map<String, String> getSettings(String sectionName) {
    return this.getConfig().getSettings(sectionName);
  }


  private String mSource;


  @Override
  public String getSource() {
    return mSource;
  }
}
