package io.ddf.types;

import com.google.common.base.Strings;

public abstract class AGloballyAddressable implements IGloballyAddressable {

  public static String getUri(IGloballyAddressable obj) {
    if(Strings.isNullOrEmpty(obj.getName())) {
      return null;
    } else {
      return String.format("%s://%s/%s/%s", obj.getGlobalObjectType(), obj
                      .getEngineName(), obj.getNamespace(), obj.getName());
    }
  }

  @Override
  public String getUri() {
    return AGloballyAddressable.getUri(this);
  }
}
