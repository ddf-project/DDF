package io.ddf.types;


/**
 * Interface for objects that are globally addressable by namespace and name
 */
public interface IGloballyAddressable {
  String getNamespace();

  void setNamespace(String namespace);

  String getName();

  void setName(String name);

  String getUri();

  String getGlobalObjectType();
}
