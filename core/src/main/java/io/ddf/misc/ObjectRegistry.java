package io.ddf.misc;


import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.ddf.IDDFManager.IGloballyAddressableObjectRegistry;
import io.ddf.types.IGloballyAddressable;
import com.google.common.base.Strings;

/**
 * Convenience class to hold a global registry of all registered objects in our system. TODO: Provide some kind of
 * persistence/garbage collection for long-running processes.
 */
public class ObjectRegistry implements IGloballyAddressableObjectRegistry {
  private final Map<String, IGloballyAddressable> mRegistryMap = new ConcurrentHashMap<String, IGloballyAddressable>();


  private String getKeyFor(IGloballyAddressable obj) {
    if (obj == null) return "null";
    return this.getKeyFor(obj, obj.getNamespace(), obj.getName());
  }

  private String getKeyFor(String namespace, String name) {
    return this.getKeyFor(null, namespace, name);
  }

  private String getKeyFor(IGloballyAddressable obj, String namespace, String name) {
    if (Strings.isNullOrEmpty(namespace)) {
      if (obj != null) namespace = obj.getNamespace();
    }
    if (Strings.isNullOrEmpty(namespace)) {
      namespace = "null";
    }

    if (Strings.isNullOrEmpty(name)) {
      if (obj != null) name = obj.getName();
    }
    if (Strings.isNullOrEmpty(name)) {
      name = "null";
    }

    return String.format("%s::%s", namespace, name);
  }

  @Override
  public void unregister(IGloballyAddressable obj) {
    mRegistryMap.remove(this.getKeyFor(obj));
  }

  @Override
  public void unregister(String namespace, String name) {
    mRegistryMap.remove(this.getKeyFor(namespace, name));
  }

  @Override
  public void register(IGloballyAddressable obj) {
    mRegistryMap.put(this.getKeyFor(obj), obj);
  }

  @Override
  public void register(IGloballyAddressable obj, String namespace, String name) {
    mRegistryMap.put(this.getKeyFor(obj, namespace, name), obj);
  }

  @Override
  public boolean contains(String namespace, String name) {
    return mRegistryMap.containsKey(this.getKeyFor(namespace, name));
  }

  @Override
  public IGloballyAddressable retrieve(String namespace, String name) {
    return mRegistryMap.get(this.getKeyFor(namespace, name));
  }

  @Override
  public void unregisterAll() {
    mRegistryMap.clear();
  }

  @Override
  public Collection<String> getKeys() {
    return mRegistryMap.keySet();
  }

  @Override
  public Collection<IGloballyAddressable> getObjects() {
    return mRegistryMap.values();
  }
}
