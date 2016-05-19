package io.ddf.types;


import io.ddf.misc.Config;

/** Default implementation for AGloballyAddressable
 */
public class GloballyAddressable extends AGloballyAddressable {

  private String nameSpace = Config.getConfigHandler().getSection(Config.ConfigConstant.SECTION_GLOBAL.toString())
      .get("nameSpace");

  @Override
  public String getGlobalObjectType() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void setNamespace(String nameSpace) {
    this.nameSpace = nameSpace;
  }

  @Override
  public String getNamespace() {
    return this.nameSpace;
  }
}
