package io.ddf.types;


import io.ddf.exception.DDFException;

/**
 * Interface for objects that are globally addressable by namespace and name
 */
public interface IGloballyAddressable {
  String getNamespace();

  void setNamespace(String namespace);

  String getName();

  void setName(String name) throws DDFException;

  String getUri();

  String getGlobalObjectType();
}
