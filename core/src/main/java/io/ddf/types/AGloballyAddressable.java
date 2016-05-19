package io.ddf.types;

import com.google.common.base.Strings;
import io.ddf.misc.Config;

import java.util.UUID;

public abstract class AGloballyAddressable implements IGloballyAddressable {

  private String nameSpace = Config.getConfigHandler().getSection(Config.ConfigConstant.SECTION_GLOBAL.toString())
      .get("nameSpace");

  protected UUID uuid =  UUID.randomUUID();

  protected String name = null;

  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  public UUID getUUID() {
    return this.uuid;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  @Override
  public void setNamespace(String nameSpace) {
    this.nameSpace = nameSpace;
  }

  @Override
  public String getNamespace() {
    return this.nameSpace;
  }

  public static String getUri(IGloballyAddressable obj) {
    if(Strings.isNullOrEmpty(obj.getName())) {
      return null;
    } else {
      return String.format("%s://%s/%s", obj.getGlobalObjectType(), obj.getNamespace(), obj.getName());
    }
  }

  @Override
  public String getUri() {
    return AGloballyAddressable.getUri(this);
  }
}
