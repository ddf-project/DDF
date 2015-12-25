package io.ddf.types;

import com.google.common.base.Strings;

import java.util.UUID;

public abstract class AGloballyAddressable implements IGloballyAddressable {

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
