package io.ddf.types;


import io.ddf.exception.DDFException;

import java.util.UUID;

/**
 * Interface for objects that are globally addressable by namespace and name
 */
public interface IGloballyAddressable {

  UUID getUUID();

  void setUUID(UUID uuid);

  String getNamespace();

  void setNamespace(String namespace);

  String getName();

  void setName(String name) throws DDFException;

  String getUri();

  default String getGlobalObjectType() {
    return this.getClass().getSimpleName();
  }
}
