package io.ddf.types;


public abstract class AGloballyAddressable implements IGloballyAddressable {

  public static String getUri(IGloballyAddressable obj) {
    if(obj.getName() == null || obj.getName().isEmpty()) {
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
