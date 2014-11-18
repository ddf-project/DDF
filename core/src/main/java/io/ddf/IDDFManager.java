package io.ddf;


import io.ddf.exception.DDFException;
import io.ddf.types.IGloballyAddressable;

import java.util.Collection;


/**

 */
public interface IDDFManager {
  void startup();

  void shutdown();

  /**
   * Returns the DDF engine name of a particular implementation, e.g., "spark".
   *
   * @return
   */
  String getEngine();

  String getNamespace() throws DDFException;

  void setNamespace(String namespace);


  public interface IGloballyAddressableObjectRegistry {
    boolean contains(String namespace, String name);

    IGloballyAddressable retrieve(String namespace, String name);

    void register(IGloballyAddressable obj);

    void register(IGloballyAddressable obj, String namespace, String name);

    void unregister(String namespace, String name);

    void unregister(IGloballyAddressable obj);

    void unregisterAll();

    Collection<IGloballyAddressable> getObjects();

    Collection<String> getKeys(); // useful for debugging
  }
}
