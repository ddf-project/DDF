package io.ddf;


import com.google.common.base.Strings;
import io.ddf.exception.DDFException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by huandao on 6/11/15.
 */
public class DDFCache {

  private Map<String, DDF> mDDFs = new ConcurrentHashMap<String, DDF>();
  private Map<String, String> mUris = new ConcurrentHashMap<String, String>();

  public void addDDF(DDF ddf) throws DDFException {
    System.out.println(">>>> ddf.uuid = " + ddf.getUUID());

    mDDFs.put(ddf.getUUID(), ddf);
  }

  public void removeDDF(DDF ddf) throws DDFException {
    mDDFs.remove(ddf.getUUID());
    if(ddf.getUri() != null) {
      mUris.remove(ddf.getUri());
    }
  }

  public DDF[] listDDFs() {
    return this.mDDFs.values().toArray(new DDF[] {});
  }

  public DDF getDDF(String uuid) throws DDFException {
    DDF ddf = mDDFs.get(uuid);
    if(ddf == null) {
      throw new DDFException(String.format("Cannot find ddf with uuid %s", uuid));

    } else {
      return ddf;
    }
  }

  public boolean hasDDF(String uuid) {
    DDF ddf = mDDFs.get(uuid);
    return ddf != null;
  }

  public DDF getDDFByName(String name) throws DDFException {
    for(DDF ddf: mDDFs.values()) {
      if(!Strings.isNullOrEmpty(ddf.getName()) && ddf.getName().equals(name)) {
        return ddf;
      }
    }
    throw new DDFException(String.format("Cannot find ddf with name %s", name));
  }

  public synchronized void setDDFName(DDF ddf, String name) throws DDFException {
    if(!Strings.isNullOrEmpty(name)) {
      ddf.setName(name);
      this.mUris.put(ddf.getUri(), ddf.getUUID());
    } else {
      throw new DDFException(String.format("DDF's name cannot be null or empty"));
    }
  }

  public synchronized void setDDFUUID(DDF ddf, String uuid) throws DDFException {
    if(this.hasDDF(uuid)) {
      throw new DDFException(String.format("DDF with uuid %s already exists", uuid));
    } else {
      ddf.setUUID(uuid);
      if(ddf.getUri() != null) {
        mUris.put(ddf.getUri(), ddf.getUUID());
      }
    }
  }

  public DDF getDDFByUri(String uri) throws DDFException {
    String uuid = this.mUris.get(uri);
    if(Strings.isNullOrEmpty(uuid)) {
      throw new DDFException(String.format("Cannot find ddf with uri %s", uri));
    }
    return this.getDDF(uuid);
  }
}
