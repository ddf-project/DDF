package io.ddf.types;


import io.ddf.exception.DDFException;

import java.util.UUID;

/**
 * Interface for objects that are globally addressable by namespace and name
 */
public interface IGloballyAddressable {

  UUID getUUID();

  String getNamespace();

  void setNamespace(String namespace);

  String getName();

  String getUri();

  String getGlobalObjectType();
}
