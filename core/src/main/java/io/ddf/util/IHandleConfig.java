package io.ddf.util;


import io.ddf.util.ConfigHandler.Configuration;
import io.ddf.util.ConfigHandler.Configuration.Section;

import java.util.Map;

public interface IHandleConfig {

  /**
   * Returns a file name, URI, or some other description of where the configuration info comes from.
   *
   * @return
   */
  public String getSource();

  /**
   * Loads/reloads the config file
   *
   * @param configDirectory
   * @return the loaded {@link Configuration} object
   * @throws Exception
   */
  public Configuration loadConfig() throws Exception;

  public Configuration getConfig();

  public void setConfig(Configuration theConfig);

  public Map<String, Section> getSections();

  public Section getSection(String sectionName);

  public Map<String, String> getSettings(String sectionName);

  public String getValue(String sectionName, String key);
}
